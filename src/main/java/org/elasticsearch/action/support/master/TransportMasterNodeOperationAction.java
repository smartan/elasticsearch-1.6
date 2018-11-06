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

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeOperationAction<Request extends MasterNodeOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    final String executor;

    protected TransportMasterNodeOperationAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, actionName, threadPool, actionFilters);
        this.transportService = transportService;
        this.clusterService = clusterService;

        this.executor = executor();

        transportService.registerHandler(actionName, new TransportHandler());
    }

    protected abstract String executor();

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws ElasticsearchException;

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    protected void processBeforeDelegationToMaster(Request request, ClusterState state) {

    }

    @Override
    protected boolean forceThreadedListener() {
        // since the callback is async, we typically can get called from within an event in the cluster service
        // or something similar, so make sure we are threaded so we won't block it.
        return true;
    }

    /**
     * Master 节点执行创建索引
     * @param request    创建索引的请求 Request
     * @param listener   ActionListener
     */
    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        innerExecute(request, listener, new ClusterStateObserver(clusterService, request.masterNodeTimeout(), logger), false);
    }

    /**
     * Master节点内部创建索引逻辑
     * @param request    Request
     * @param listener   ActionListener
     * @param observer   ClusterStateObserver
     * @param retrying   boolean
     */
    private void innerExecute(final Request request, final ActionListener<Response> listener, final ClusterStateObserver observer, final boolean retrying) {
        final ClusterState clusterState = observer.observedState();
        final DiscoveryNodes nodes = clusterState.nodes();
        // 判断当前节点是否master, 因为索引创建过程必须在master节点上完成
        if (nodes.localNodeMaster() || localExecute(request)) {
            // check for block, if blocked, retry, else, execute locally
            // TransportCreateIndexAction.checkBlock()
            final ClusterBlockException blockException = checkBlock(request, clusterState);
            if (blockException != null) {
                if (!blockException.retryable()) {
                    listener.onFailure(blockException);
                    return;
                }
                logger.trace("can't execute due to a cluster block: [{}], retrying", blockException);
                observer.waitForNextChange(
                        new ClusterStateObserver.Listener() {
                            @Override
                            public void onNewClusterState(ClusterState state) {
                                // 集群状态发生了改变, 重新执行该方法创建index
                                innerExecute(request, listener, observer, false);
                            }

                            @Override
                            public void onClusterServiceClose() {
                                listener.onFailure(blockException);
                            }

                            @Override
                            public void onTimeout(TimeValue timeout) {
                                listener.onFailure(blockException);
                            }
                        }, new ClusterStateObserver.ValidationPredicate() {
                            @Override
                            protected boolean validate(ClusterState newState) {
                                ClusterBlockException blockException = checkBlock(request, newState);
                                return (blockException == null || !blockException.retryable());
                            }
                        }
                );

            } else {
                try {
                    // 如果没有block
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                masterOperation(request, clusterService.state(), listener);
                            } catch (Throwable e) {
                                listener.onFailure(e);
                            }
                        }
                    });
                } catch (Throwable t) {
                    listener.onFailure(t);
                }
            }
        } else {
            // 如果当前节点不是master且集群没有master
            if (nodes.masterNode() == null) {
                // 不重试, 直接抛出异常
                if (retrying) {
                    listener.onFailure(new MasterNotDiscoveredException());
                } else {
                    logger.debug("no known master node, scheduling a retry");
                    observer.waitForNextChange(
                            new ClusterStateObserver.Listener() {
                                @Override
                                public void onNewClusterState(ClusterState state) {
                                    // 集群状态发生了改变, 重新执行该方法创建index
                                    innerExecute(request, listener, observer, true);
                                }

                                @Override
                                public void onClusterServiceClose() {
                                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                                }

                                @Override
                                public void onTimeout(TimeValue timeout) {
                                    listener.onFailure(new MasterNotDiscoveredException("waited for [" + timeout + "]"));
                                }
                            }, new ClusterStateObserver.ChangePredicate() {
                                @Override
                                public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus,
                                                     ClusterState newState, ClusterState.ClusterStateStatus newStatus) {
                                    return newState.nodes().masterNodeId() != null;
                                }

                                @Override
                                public boolean apply(ClusterChangedEvent event) {
                                    return event.nodesDelta().masterNodeChanged();
                                }
                            }
                    );
                }
                return;
            }
            // empty
            processBeforeDelegationToMaster(request, clusterState);

            // CreateIndexAction.NAME = indices:admin/create
            // 将建索引请求发送给master节点, 接收的handler为TransportHandler
            transportService.sendRequest(nodes.masterNode(), actionName, request, new BaseTransportResponseHandler<Response>() {
                @Override
                public Response newInstance() {
                    return newResponse();
                }

                @Override
                public void handleResponse(Response response) {
                    listener.onResponse(response);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                // 处理异常
                @Override
                public void handleException(final TransportException exp) {
                    if (exp.unwrapCause() instanceof ConnectTransportException) {
                        // we want to retry here a bit to see if a new master is elected
                        logger.debug("connection exception while trying to forward request to master node [{}], scheduling a retry. Error: [{}]",
                                nodes.masterNode(), exp.getDetailedMessage());
                        observer.waitForNextChange(new ClusterStateObserver.Listener() {
                                                       @Override
                                                       public void onNewClusterState(ClusterState state) {
                                                           innerExecute(request, listener, observer, false);
                                                       }

                                                       @Override
                                                       public void onClusterServiceClose() {
                                                           listener.onFailure(new NodeClosedException(clusterService.localNode()));
                                                       }

                                                       @Override
                                                       public void onTimeout(TimeValue timeout) {
                                                           listener.onFailure(new MasterNotDiscoveredException());
                                                       }
                                                   }, new ClusterStateObserver.EventPredicate() {
                                                       @Override
                                                       public boolean apply(ClusterChangedEvent event) {
                                                           return event.nodesDelta().masterNodeChanged();
                                                       }
                                                   }
                        );
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
        }
    }

    // 对应innerExecute里的的actionName
    // transportService.registerHandler(actionName, new TransportHandler());
    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            //
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }
    }
}
