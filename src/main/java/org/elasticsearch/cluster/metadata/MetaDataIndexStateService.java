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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 打开/关闭 索引服务
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetaDataIndexStateService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    /**
     * 关闭索引操作
     * @param request   CloseIndexClusterStateUpdateRequest 关闭索引请求
     * @param listener  ActionListener
     */
    public void closeIndex(final CloseIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new ElasticsearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());

        // 执行InternalClusterService.UpdateTask.run()方法, 即调用task的execute方法
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString, Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            /**
             * 执行更新集群状态的操作, 关闭索引
             * @param currentState  ClusterState  当前集群状态
             * @return  ClusterState
             */
            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToClose = new ArrayList<>();
                for (String index : request.indices()) {
                    // 请求的索引元数据
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }

                    // 如果要关闭的索引状态不是close, 则加入到待关闭的索引list中
                    if (indexMetaData.state() != IndexMetaData.State.CLOSE) {
                        // 索引路由表
                        IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                        for (IndexShardRoutingTable shard : indexRoutingTable) {
                            // 是否在API创建后分配了此分片组主分片
                            if (!shard.primaryAllocatedPostApi()) {
                                throw new IndexPrimaryShardNotAllocatedException(new Index(index));
                            }
                        }
                        indicesToClose.add(index);
                    }
                }

                // 没有要关闭的索引, 什么都不做
                if (indicesToClose.isEmpty()) {
                    return currentState;
                }

                logger.info("closing indices [{}]", indicesAsString);

                // 构建新的meta data
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());

                // 重新构建meta data builder和blocks builder, 加上closed标签
                for (String index : indicesToClose) {
                    mdBuilder.put(IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.CLOSE));
                    blocksBuilder.addIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                // 使用meta data builder和blocks builder, 更新集群状态
                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                // 从路由表routing table中删掉请求的索引
                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (String index : indicesToClose) {
                    rtBuilder.remove(index);
                }

                // 重写构建路由表routing table, 使用更新后的cluster state和更新后的routing table
                // 在这里要allocation shard
                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());

                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }
        });
    }

    /**
     * 打开索引操作
     * @param request   OpenIndexClusterStateUpdateRequest  打开索引请求
     * @param listener  ActionListener
     */
    public void openIndex(final OpenIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new ElasticsearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());

        // 执行InternalClusterService.UpdateTask.run()方法, 即调用task的execute方法
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString, Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToOpen = new ArrayList<>();
                for (String index : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }
                    // 如果要Open的索引已经是Open状态了, 就过滤掉
                    if (indexMetaData.state() != IndexMetaData.State.OPEN) {
                        indicesToOpen.add(index);
                    }
                }

                // 如果没有要打开的索引, 什么都不做
                if (indicesToOpen.isEmpty()) {
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                // 重新构建meta data builder和blocks builder, 加上open标签, 移除closed block
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToOpen) {
                    mdBuilder.put(IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN));
                    blocksBuilder.removeIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                // 使用meta data builder和blocks builder, 更新集群状态
                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                // 从路由表routing table中删掉请求的索引
                RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
                for (String index : indicesToOpen) {
                    rtBuilder.addAsRecovery(updatedState.metaData().index(index));
                }

                // 重写构建路由表routing table, 使用更新后的cluster state和更新后的routing table
                // 在这里要allocation shard
                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());

                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }
        });
    }

}
