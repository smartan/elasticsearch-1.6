/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class TransportNodesShutdownAction extends TransportMasterNodeOperationAction<NodesShutdownRequest, NodesShutdownResponse> {

    public static final String SHUTDOWN_NODE_ACTION_NAME = NodesShutdownAction.NAME + "[n]";

    private final Node node;
    private final ClusterName clusterName;
    private final boolean disabled;
    private final TimeValue delay;

    @Inject
    public TransportNodesShutdownAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        Node node, ClusterName clusterName, ActionFilters actionFilters) {
        super(settings, NodesShutdownAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.node = node;
        this.clusterName = clusterName;
        this.disabled = settings.getAsBoolean("action.disable_shutdown", componentSettings.getAsBoolean("disabled", false));
        this.delay = componentSettings.getAsTime("delay", TimeValue.timeValueMillis(200));

        this.transportService.registerHandler(SHUTDOWN_NODE_ACTION_NAME, new NodeShutdownRequestHandler());
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterBlockException checkBlock(NodesShutdownRequest request, ClusterState state) {
        // Stopping a node impacts the cluster state, so we check for the METADATA_WRITE block here
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected NodesShutdownRequest newRequest() {
        return new NodesShutdownRequest();
    }

    @Override
    protected NodesShutdownResponse newResponse() {
        return new NodesShutdownResponse();
    }

    @Override
    protected void processBeforeDelegationToMaster(NodesShutdownRequest request, ClusterState state) {
        String[] nodesIds = request.nodesIds;
        if (nodesIds != null) {
            for (int i = 0; i < nodesIds.length; i++) {
                // replace the _local one, since it looses its meaning when going over to the master...
                if ("_local".equals(nodesIds[i])) {
                    nodesIds[i] = state.nodes().localNodeId();
                }
            }
        }
    }

    /**
     * 关闭node 节点的master 操作
     * @param request   NodesShutdownRequest
     * @param state     ClusterState
     * @param listener  ActionListener
     * @throws ElasticsearchException   ElasticsearchException
     */
    @Override
    protected void masterOperation(final NodesShutdownRequest request, final ClusterState state, final ActionListener<NodesShutdownResponse> listener) throws ElasticsearchException {
        if (disabled) {
            throw new ElasticsearchIllegalStateException("Shutdown is disabled");
        }
        final ObjectOpenHashSet<DiscoveryNode> nodes = new ObjectOpenHashSet<>();
        // 关闭全部节点
        if (state.nodes().isAllNodes(request.nodesIds)) {
            logger.info("[cluster_shutdown]: requested, shutting down in [{}]", request.delay);
            // 添加 data 节点
            nodes.addAll(state.nodes().dataNodes().values());
            // 添加 master 节点
            nodes.addAll(state.nodes().masterNodes().values());
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(request.delay.millis());
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    // first, stop the cluster service
                    logger.trace("[cluster_shutdown]: stopping the cluster service so no re-routing will occur");
                    clusterService.stop();

                    final CountDownLatch latch = new CountDownLatch(nodes.size());
                    for (ObjectCursor<DiscoveryNode> cursor : nodes) {
                        final DiscoveryNode node = cursor.value;
                        // 如果要shutdown 的节点是master, 则不做操作
                        if (node.id().equals(state.nodes().masterNodeId())) {
                            // don't shutdown the master yet...
                            latch.countDown();
                        } else {
                            logger.trace("[cluster_shutdown]: sending shutdown request to [{}]", node);
                            transportService.sendRequest(node, SHUTDOWN_NODE_ACTION_NAME, new NodeShutdownRequest(request), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                                @Override
                                public void handleResponse(TransportResponse.Empty response) {
                                    logger.trace("[cluster_shutdown]: received shutdown response from [{}]", node);
                                    latch.countDown();
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    logger.warn("[cluster_shutdown]: received failed shutdown response from [{}]", exp, node);
                                    latch.countDown();
                                }
                            });
                        }
                    }
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    logger.info("[cluster_shutdown]: done shutting down all nodes except master, proceeding to master");

                    // now, kill the master
                    logger.trace("[cluster_shutdown]: shutting down the master [{}]", state.nodes().masterNode());
                    transportService.sendRequest(state.nodes().masterNode(), SHUTDOWN_NODE_ACTION_NAME, new NodeShutdownRequest(request), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            logger.trace("[cluster_shutdown]: received shutdown response from master");
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.warn("[cluster_shutdown]: received failed shutdown response master", exp);
                        }
                    });
                }
            });
            t.start();
        } else {
            // 解析要关闭的节点信息
            final String[] nodesIds = state.nodes().resolveNodesIds(request.nodesIds);
            logger.info("[partial_cluster_shutdown]: requested, shutting down [{}] in [{}]", nodesIds, request.delay);

            for (String nodeId : nodesIds) {
                final DiscoveryNode node = state.nodes().get(nodeId);
                if (node != null) {
                    nodes.add(node);
                }
            }

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(request.delay.millis());
                    } catch (InterruptedException e) {
                        // ignore
                    }

                    final CountDownLatch latch = new CountDownLatch(nodesIds.length);
                    // 遍历要关闭的节点
                    for (String nodeId : nodesIds) {
                        final DiscoveryNode node = state.nodes().get(nodeId);
                        if (node == null) {
                            logger.warn("[partial_cluster_shutdown]: no node to shutdown for node_id [{}]", nodeId);
                            latch.countDown();
                            continue;
                        }

                        logger.trace("[partial_cluster_shutdown]: sending shutdown request to [{}]", node);
                        transportService.sendRequest(
                                node, // 发送到的节点
                                SHUTDOWN_NODE_ACTION_NAME, // 注册的action, 通过这个action 去找对应的handler
                                new NodeShutdownRequest(request), // 封装的请求
                                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {  // 处理响应的handler

                            @Override
                            public void handleResponse(TransportResponse.Empty response) {
                                logger.trace("[partial_cluster_shutdown]: received shutdown response from [{}]", node);
                                latch.countDown();
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.warn("[partial_cluster_shutdown]: received failed shutdown response from [{}]", exp, node);
                                latch.countDown();
                            }
                        });
                    }

                    // await() 等待节点关闭
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        // ignore
                    }

                    logger.info("[partial_cluster_shutdown]: done shutting down [{}]", ((Object) nodesIds));
                }
            });
            t.start();
        }
        listener.onResponse(new NodesShutdownResponse(clusterName, nodes.toArray(DiscoveryNode.class)));
    }

    /**
     * 该handler 已经注册到 SHUTDOWN_NODE_ACTION_NAME action 中
     */
    private class NodeShutdownRequestHandler extends BaseTransportRequestHandler<NodeShutdownRequest> {

        @Override
        public NodeShutdownRequest newInstance() {
            return new NodeShutdownRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final NodeShutdownRequest request, TransportChannel channel) throws Exception {
            if (disabled) {
                throw new ElasticsearchIllegalStateException("Shutdown is disabled");
            }
            logger.info("shutting down in [{}]", delay);
            // 新启动一个线程, 执行org.tanukisoftware.wrapper.WrapperManager.stopAndReturn 方法或者执行node.close()
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(delay.millis());
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    if (!request.exit) {
                        logger.info("initiating requested shutdown (no exit)...");
                        try {
                            node.close();
                        } catch (Exception e) {
                            logger.warn("Failed to shutdown", e);
                        }
                        return;
                    }
                    boolean shutdownWithWrapper = false;
                    if (System.getProperty("elasticsearch-service") != null) {
                        try {
                            Class wrapperManager = settings.getClassLoader().loadClass("org.tanukisoftware.wrapper.WrapperManager");
                            logger.info("initiating requested shutdown (using service)");
                            wrapperManager.getMethod("stopAndReturn", int.class).invoke(null, 0);
                            shutdownWithWrapper = true;
                        } catch (Throwable e) {
                            logger.error("failed to initial shutdown on service wrapper", e);
                        }
                    }
                    if (!shutdownWithWrapper) {
                        logger.info("initiating requested shutdown...");
                        try {
                            node.close();
                        } catch (Exception e) {
                            logger.warn("Failed to shutdown", e);
                        } finally {
                            // make sure we initiate the shutdown hooks, so the Bootstrap#main thread will exit
                            System.exit(0);
                        }
                    }
                }
            });
            t.start();

            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    static class NodeShutdownRequest extends TransportRequest {

        boolean exit;

        NodeShutdownRequest() {
        }

        NodeShutdownRequest(NodesShutdownRequest request) {
            super(request);
            this.exit = request.exit();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            exit = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(exit);
        }
    }
}
