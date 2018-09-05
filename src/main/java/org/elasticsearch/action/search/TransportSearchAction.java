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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.type.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.search.SearchType.*;

/**
 *
 */
public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private final ClusterService clusterService;
    private final TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction;
    private final TransportSearchQueryThenFetchAction queryThenFetchAction;
    private final TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction;
    private final TransportSearchQueryAndFetchAction queryAndFetchAction;
    private final TransportSearchScanAction scanAction;
    private final TransportSearchCountAction countAction;
    private final boolean optimizeSingleShard;

    @Inject
    public TransportSearchAction(Settings settings, ThreadPool threadPool,
                                 TransportService transportService, ClusterService clusterService,
                                 TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction,
                                 TransportSearchQueryThenFetchAction queryThenFetchAction,
                                 TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction,
                                 TransportSearchQueryAndFetchAction queryAndFetchAction,
                                 TransportSearchScanAction scanAction,
                                 TransportSearchCountAction countAction, ActionFilters actionFilters) {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters);
        this.clusterService = clusterService;
        this.dfsQueryThenFetchAction = dfsQueryThenFetchAction;
        this.queryThenFetchAction = queryThenFetchAction;
        this.dfsQueryAndFetchAction = dfsQueryAndFetchAction;
        this.queryAndFetchAction = queryAndFetchAction;
        this.scanAction = scanAction;
        this.countAction = countAction;

        this.optimizeSingleShard = componentSettings.getAsBoolean("optimize_single_shard", true);


    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // optimize search type for cases where there is only one shard group to search on
        if (optimizeSingleShard && searchRequest.searchType() != SCAN && searchRequest.searchType() != COUNT) {
            try {
                ClusterState clusterState = clusterService.state();
                String[] concreteIndices = clusterState.metaData().concreteIndices(searchRequest.indicesOptions(), searchRequest.indices());
                Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(searchRequest.routing(), searchRequest.indices());
                int shardCount = clusterService.operationRouting().searchShardsCount(clusterState, searchRequest.indices(), concreteIndices, routingMap, searchRequest.preference());
                if (shardCount == 1) {
                    // if we only have one group, then we always want Q_A_F, no need for DFS, and no need to do THEN since we hit one shard
                    searchRequest.searchType(QUERY_AND_FETCH);
                }
            } catch (IndexMissingException|IndexClosedException e) {
                // ignore these failures, we will notify the search response if its really the case from the actual action
            } catch (Exception e) {
                logger.debug("failed to optimize search type, continue as normal", e);
            }
        }

        // 根据search type,执行对应的Action
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            // 计算分布式词频以获得更准确的评分
            dfsQueryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_THEN_FETCH) {
            // 在第一阶段,查询被转发到所有涉及的分片,每个分片执行搜索并生成该分片的本地结果排序列表,每个分片都向协调节点返回足够的信息,以允许它合并并将分片级别结果重新排序为具有最大长度大小的全局排序结果集
            // 在第二阶段期间,协调节点仅从相关分片请求文档内容(以及突出显示的片段,如果有的话)
            queryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.DFS_QUERY_AND_FETCH) {
            // 请求针对单分片时,计算分布式词频以获得更准确的评分
            dfsQueryAndFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_AND_FETCH) {
            // 当query_then_fetch请求仅针对单个分片时,会自动选择该模式
            queryAndFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.SCAN) {
            scanAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.COUNT) {
            countAction.execute(searchRequest, listener);
        }
    }

    @Override
    public SearchRequest newRequestInstance() {
        return new SearchRequest();
    }
}
