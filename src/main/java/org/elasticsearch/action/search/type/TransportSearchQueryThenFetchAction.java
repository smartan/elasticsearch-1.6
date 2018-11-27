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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.ScoreDoc;
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
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * QUERY_THEN_FETCH 包含一阶段和二阶段逻辑
 */
public class TransportSearchQueryThenFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                               SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController, ActionFilters actionFilters) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters);
    }

    /**
     * QUERY_THEN_FETCH 搜索入口
     * @param searchRequest     SearchRequest
     * @param listener          ActionListener
     */
    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // 执行Q_T_F
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QuerySearchResultProvider> {

        final AtomicArray<FetchSearchResult> fetchResults;
        final AtomicArray<IntArrayList> docIdsToLoad;

        /**
         * Q_T_F 内部类构造方法
         * @param request   SearchRequest
         * @param listener  ActionListener
         */
        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            // 调用父类的构造方法, 获取要查询的Cluster Index Routing Shard Preference
            super(request, listener);
            // 第二阶段 <ShardIndex, FetchResult>
            fetchResults = new AtomicArray<>(firstResults.length());
            // 第一阶段 <ShardIndex, DocId>
            docIdsToLoad = new AtomicArray<>(firstResults.length());
        }

        @Override
        protected String firstPhaseName() {
            return "query";
        }

        /**
         * QUERY_THEN_FETCH 第一阶段请求
         * @param node      shard 分片所在的节点
         * @param request   ShardSearchTransportRequest
         * @param listener  SearchServiceListener 执行listener.onResult()处理第一阶段结果
         */
        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, SearchServiceListener<QuerySearchResultProvider> listener) {
            searchService.sendExecuteQuery(node, request, listener);
        }


        /**
         * QUERY_THEN_FETCH 第二阶段 FETCH 和 MERGE 过程
         * @throws Exception Exception
         */
        @Override
        protected void moveToSecondPhase() throws Exception {
            boolean useScroll = !useSlowScroll && request.scroll() != null;
            // 对第一阶段每个shard的Query结果进行排序, 获取merge后的top docs
            sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults);

            // 填充要fetch的doc id
            searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

            // 判断query结果是否为空
            if (docIdsToLoad.asList().isEmpty()) {
                //Query结果为空, 直接合并query和fetch, 返回响应结果
                finishHim();
                return;
            }

            // 获取每个shard index最后一个doc
            final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(
                    request, sortedShardList, firstResults.length()
            );
            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());

            // 遍历每个shard, 去对应的各个节点上获取文档数据
            for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
                // query 结果
                QuerySearchResultProvider queryResult = firstResults.get(entry.index);
                // 对应的节点
                DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
                // 创建fetch请求
                ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult.queryResult(), entry, lastEmittedDocPerShard);
                // 执行fetch
                executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
            }
        }

        /**
         * 执行 QUERY_THEN_FETCH 的 fetch 阶段
         * @param shardIndex    int
         * @param shardTarget   SearchShardTarget
         * @param counter   AtomicInteger
         * @param fetchSearchRequest    ShardFetchSearchRequest
         * @param node  DiscoveryNode
         */
        void executeFetch(final int shardIndex, final SearchShardTarget shardTarget, final AtomicInteger counter, final ShardFetchSearchRequest fetchSearchRequest, DiscoveryNode node) {
            // 发起fetch请求
            searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override
                public void onResult(FetchSearchResult result) {
                    result.shardTarget(shardTarget);
                    // set fetch 结果
                    fetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        // 获取 fetch 结果后执行 merge 并返回响应
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // the search context might not be cleared on the node where the fetch was executed for example
                    // because the action was rejected by the thread pool. in this case we need to send a dedicated
                    // request to clear the search context. by setting docIdsToLoad to null, the context will be cleared
                    // in TransportSearchTypeAction.releaseIrrelevantSearchContexts() after the search request is done.
                    docIdsToLoad.set(shardIndex, null);
                    onFetchFailure(t, fetchSearchRequest, shardIndex, shardTarget, counter);
                }
            });
        }

        void onFetchFailure(Throwable t, ShardFetchSearchRequest fetchSearchRequest, int shardIndex, SearchShardTarget shardTarget, AtomicInteger counter) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute fetch phase", t, fetchSearchRequest.id());
            }
            this.addShardFailure(shardIndex, shardTarget, t);
            successfulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                // 执行merge
                finishHim();
            }
        }

        private void finishHim() {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
                @Override
                public void doRun() throws IOException {
                    // 对结果进行merge, 对每一个一阶段结果, 填充fetch到的数据
                    final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);
                    String scrollId = null;
                    if (request.scroll() != null) {
                        scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults, null);
                    }
                    // 封装搜索响应,并对hits进行解压
                    listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                    releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", t, buildShardFailures());
                        if (logger.isDebugEnabled()) {
                            logger.debug("failed to reduce search", failure);
                        }
                        super.onFailure(failure);
                    } finally {
                        releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
                    }
                }
            });
        }
    }
}
