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

package org.elasticsearch.action.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.WriteFailureException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the index operation.
 * <p/>
 * <p>Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to <tt>true</tt>, will automatically create an index if one does not exists.
 * Defaults to <tt>true</tt>.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to <tt>true</tt>.
 * </ul>
 */
public class TransportIndexAction extends TransportShardReplicationOperationAction<IndexRequest, IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final TransportCreateIndexAction createIndexAction;

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters) {
        super(settings, IndexAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
        this.createIndexAction = createIndexAction;
        this.mappingUpdatedAction = mappingUpdatedAction;
        // action.auto_create_index 参数
        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
    }

    /**
     * 执行创建索引流程
     * @param request     IndexRequest
     * @param listener    ActionListener
     */
    @Override
    protected void doExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        // 如果允许自动创建索引
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(request);
            // 索引名 index
            createIndexRequest.index(request.index());
            // 索引类型 type
            createIndexRequest.mapping(request.type());
            // 创建索引的原因
            createIndexRequest.cause("auto(index api)");
            // 超时时间
            createIndexRequest.masterNodeTimeout(request.timeout());

            // TransportMasterNodeOperationAction.doExecute()
            // 创建索引
            createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    // 向索引中添加文档信息
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    // 如果是索引已经存在异常, 则继续创建索引
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(request, listener);
                        } catch (Throwable e1) {
                            listener.onFailure(e1);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(request, listener);  // 索引已经存在
        }
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected boolean resolveRequest(ClusterState state, InternalRequest request, ActionListener<IndexResponse> indexResponseActionListener) {
        MetaData metaData = clusterService.state().metaData();

        MappingMetaData mappingMd = null;
        if (metaData.hasIndex(request.concreteIndex())) {
            mappingMd = metaData.index(request.concreteIndex()).mappingOrDefault(request.request().type());
        }
        request.request().process(metaData, mappingMd, allowIdGeneration, request.concreteIndex());
        return true;
    }

    private void innerExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(request, listener); // TransportShardReplicationOperationAction.doExecute()
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected IndexRequest newRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexRequest newReplicaRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.concreteIndex(), request.request().type(), request.request().id(), request.request().routing());
    }

    /**
     * primary shard 操作
     * @param clusterState  ClusterState
     * @param shardRequest  PrimaryOperationRequest
     * @return              Tuple
     * @throws Throwable    Throwable
     */
    @Override
    protected Tuple<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
        final IndexRequest request = shardRequest.request;

        // validate, if routing is required, that we got routing
        // index meta data
        IndexMetaData indexMetaData = clusterState.metaData().index(shardRequest.shardId.getIndex());
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (request.routing() == null) {
                throw new RoutingMissingException(shardRequest.shardId.getIndex(), request.type(), request.id());
            }
        }

        // index service
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.shardId.getIndex());
        // index shard
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());

        // request转化为source  type  id  routing  parent  timestamp  ttl
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());

        long version;
        boolean created;
        try {
            Engine.IndexingOperation op;
            // 如果是 index request
            if (request.opType() == IndexRequest.OpType.INDEX) {
                Engine.Index index = indexShard.prepareIndex(sourceToParse, request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, request.canHaveDuplicates());
                if (index.parsedDoc().mappingsModified()) {
                    mappingUpdatedAction.updateMappingOnMaster(shardRequest.shardId.getIndex(), index.docMapper(), indexService.indexUUID());
                }
                // 执行index请求
                indexShard.index(index);
                version = index.version();
                op = index;
                created = index.created();
            } else {
                // 如果是 create request
                Engine.Create create = indexShard.prepareCreate(sourceToParse,
                        request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, request.canHaveDuplicates(), request.autoGeneratedId());
                if (create.parsedDoc().mappingsModified()) {
                    mappingUpdatedAction.updateMappingOnMaster(shardRequest.shardId.getIndex(), create.docMapper(), indexService.indexUUID());
                }
                // 执行create请求
                indexShard.create(create);
                version = create.version();
                op = create;
                created = true;
            }
            // _refresh 参数
            if (request.refresh()) {
                try {
                    indexShard.refresh("refresh_flag_index");
                } catch (Throwable e) {
                    // ignore
                }
            }
            // update the version on the request, so it will be used for the replicas
            request.version(version);
            request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
            assert request.versionType().validateVersionForWrites(request.version());

            // 返回响应
            IndexResponse response = new IndexResponse(shardRequest.shardId.getIndex(), request.type(), request.id(), version, created);
            return new Tuple<>(response, shardRequest.request);

        } catch (WriteFailureException e) {
            if (e.getMappingTypeToUpdate() != null) {
                DocumentMapper docMapper = indexService.mapperService().documentMapper(e.getMappingTypeToUpdate());
                if (docMapper != null) {
                    mappingUpdatedAction.updateMappingOnMaster(indexService.index().name(), docMapper, indexService.indexUUID());
                }
            }
            throw e.getCause();
        }
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        IndexRequest request = shardRequest.request;
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        if (request.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse, request.version(), request.versionType(), Engine.Operation.Origin.REPLICA, request.canHaveDuplicates());
            indexShard.index(index);
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse,
                    request.version(), request.versionType(), Engine.Operation.Origin.REPLICA, request.canHaveDuplicates(), request.autoGeneratedId());
            indexShard.create(create);
        }
        if (request.refresh()) {
            try {
                indexShard.refresh("refresh_flag_index");
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
