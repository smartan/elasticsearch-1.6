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

package org.elasticsearch.action.search.type;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.action.search.type.TransportSearchHelper.buildScrollId;

/**
 *
 */
public class TransportSearchQueryAndFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController, ActionFilters actionFilters) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // 异步执行Action
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QueryFetchSearchResult> {

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override
        protected String firstPhaseName() {
            return "query_fetch";
        }

        /**
         * QUERY_THEN_FETCH的第一阶段,执行Query和Fetch
         * @param node  要执行的节点
         * @param request  ShardSearchTransportRequest
         * @param listener SearchServiceListener
         */
        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, SearchServiceListener<QueryFetchSearchResult> listener) {
            searchService.sendExecuteFetch(node, request, listener);
        }

        /**
         * QUERY_THEN_FETCH的第二阶段,执行排序并且将结果返回给调用者
         * @throws Exception 抛出异常
         */
        @Override
        protected void moveToSecondPhase() throws Exception {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
                @Override
                public void doRun() throws IOException {
                    boolean useScroll = !useSlowScroll && request.scroll() != null;
                    // 对Query结果进行排序
                    sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults); //firstResults是第一阶段每一个shard的Query结果
                    // merge已排序结果和第一阶段结果, Query结果和Fetch结果一致
                    final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults, firstResults);
                    String scrollId = null;
                    if (request.scroll() != null) {
                        // 如果是scroll, 则需要返回scroll id
                        scrollId = buildScrollId(request.searchType(), firstResults, null);
                    }
                    // 将响应结果返回给调用者
                    listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                }

                @Override
                public void onFailure(Throwable t) {
                    ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", t, buildShardFailures());
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to reduce search", failure);
                    }
                    super.onFailure(failure);
                }
            });
        }
    }
}
