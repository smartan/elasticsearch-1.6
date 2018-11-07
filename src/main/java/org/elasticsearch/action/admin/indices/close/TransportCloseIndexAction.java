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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Close index action
 */
public class TransportCloseIndexAction extends TransportMasterNodeOperationAction<CloseIndexRequest, CloseIndexResponse> {

    private final MetaDataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportCloseIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataIndexStateService indexStateService, NodeSettingsService nodeSettingsService, ActionFilters actionFilters) {
        super(settings, CloseIndexAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.indexStateService = indexStateService;
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CloseIndexRequest newRequest() {
        return new CloseIndexRequest();
    }

    @Override
    protected CloseIndexResponse newResponse() {
        return new CloseIndexResponse();
    }

    /**
     * 重写TransportMasterNodeOperationAction.doExecute()方法
     * @param request    Request    关闭索引的请求
     * @param listener   ActionListener
     */
    @Override
    protected void doExecute(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        // 如果是master节点就在当前节点执行, 否则将请求发送到指定的master节点
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    /**
     * 重写TransportMasterNodeOperationAction.masterOperation()方法, 实现master节点关闭索引操作
     * @param request   CloseIndexRequest   关闭索引的请求
     * @param state     ClusterState    集群的状态信息
     * @param listener  ActionListener  监听器, 用于返回响应结果
     * @throws ElasticsearchException   Elasticsearch异常
     */
    @Override
    protected void masterOperation(final CloseIndexRequest request, final ClusterState state, final ActionListener<CloseIndexResponse> listener) throws ElasticsearchException {
        final String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        // 构建关闭索引请求
        CloseIndexClusterStateUpdateRequest updateRequest = new CloseIndexClusterStateUpdateRequest()   // 无参构造方法
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .indices(concreteIndices);

        // 调用IndexStateService 关闭索引
        indexStateService.closeIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new CloseIndexResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to close indices [{}]", t, concreteIndices);
                listener.onFailure(t);
            }
        });
    }
}
