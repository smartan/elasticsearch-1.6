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

package org.elasticsearch.search.controller;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class SearchPhaseController extends AbstractComponent {

    public static final Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>> QUERY_RESULT_ORDERING = new Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>>() {
        @Override
        public int compare(AtomicArray.Entry<? extends QuerySearchResultProvider> o1, AtomicArray.Entry<? extends QuerySearchResultProvider> o2) {
            int i = o1.value.shardTarget().index().compareTo(o2.value.shardTarget().index());
            if (i == 0) {
                i = o1.value.shardTarget().shardId() - o2.value.shardTarget().shardId();
            }
            return i;
        }
    };

    public static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final CacheRecycler cacheRecycler;
    private final BigArrays bigArrays;
    private final boolean optimizeSingleShard;

    private ScriptService scriptService;

    @Inject
    public SearchPhaseController(Settings settings, CacheRecycler cacheRecycler, BigArrays bigArrays, ScriptService scriptService) {
        super(settings);
        this.cacheRecycler = cacheRecycler;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.optimizeSingleShard = componentSettings.getAsBoolean("optimize_single_shard", true);
    }

    public boolean optimizeSingleShard() {
        return optimizeSingleShard;
    }

    public AggregatedDfs aggregateDfs(AtomicArray<DfsSearchResult> results) {
        ObjectObjectOpenHashMap<Term, TermStatistics> termStatistics = HppcMaps.newNoNullKeysMap();
        ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
        long aggMaxDoc = 0;
        for (AtomicArray.Entry<DfsSearchResult> lEntry : results.asList()) {
            final Term[] terms = lEntry.value.terms();
            final TermStatistics[] stats = lEntry.value.termStatistics();
            assert terms.length == stats.length;
            for (int i = 0; i < terms.length; i++) {
                assert terms[i] != null;
                TermStatistics existing = termStatistics.get(terms[i]);
                if (existing != null) {
                    assert terms[i].bytes().equals(existing.term());
                    // totalTermFrequency is an optional statistic we need to check if either one or both
                    // are set to -1 which means not present and then set it globally to -1
                    termStatistics.put(terms[i], new TermStatistics(existing.term(),
                            existing.docFreq() + stats[i].docFreq(),
                            optionalSum(existing.totalTermFreq(), stats[i].totalTermFreq())));
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }
            final boolean[] states = lEntry.value.fieldStatistics().allocated;
            final Object[] keys = lEntry.value.fieldStatistics().keys;
            final Object[] values = lEntry.value.fieldStatistics().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    String key = (String) keys[i];
                    CollectionStatistics value = (CollectionStatistics) values[i];
                    assert key != null;
                    CollectionStatistics existing = fieldStatistics.get(key);
                    if (existing != null) {
                        CollectionStatistics merged = new CollectionStatistics(
                                key, existing.maxDoc() + value.maxDoc(),
                                optionalSum(existing.docCount(), value.docCount()),
                                optionalSum(existing.sumTotalTermFreq(), value.sumTotalTermFreq()),
                                optionalSum(existing.sumDocFreq(), value.sumDocFreq())
                        );
                        fieldStatistics.put(key, merged);
                    } else {
                        fieldStatistics.put(key, value);
                    }
                }
            }
            aggMaxDoc += lEntry.value.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }

    private static long optionalSum(long left, long right) {
        return Math.min(left, right) == -1 ? -1 : left + right;
    }

    /**
     * 对Query结果进行排序
     * @param scrollSort Whether to ignore the from and sort all hits in each shard result. Only used for scroll search
     * @param resultsArr Shard result holder    第一阶段每一个shard的Query结果
     */
    public ScoreDoc[] sortDocs(boolean scrollSort, AtomicArray<? extends QuerySearchResultProvider> resultsArr) throws IOException {
        // 第一阶段结果list
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results = resultsArr.asList();
        if (results.isEmpty()) {
            return EMPTY_DOCS;
        }

        // componentSettings.getAsBoolean("optimize_single_shard", true)
        // 单分片和多分片统一的逻辑
        if (optimizeSingleShard) {
            boolean canOptimize = false;
            QuerySearchResult result = null;
            int shardIndex = -1;
            // 如果是单分片, QUERY_AND_FETCH
            if (results.size() == 1) {
                // 只有是从单分片获取结果才可以optimize
                canOptimize = true;
                // 分片查询结果
                result = results.get(0).value.queryResult();
                // 分片序号
                shardIndex = results.get(0).index;
            } else {
                // QUERY_THEN_FETCH
                // 如果仅从一个分片中命中结果, 则optimize为true, 否则为false
                // lets see if we only got hits from a single shard, if so, we can optimize...
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : results) {
                    if (entry.value.queryResult().topDocs().scoreDocs.length > 0) {
                        // 如果多个分片中有一个分片的result不为null, 则canOptimize为false
                        if (result != null) { // we already have one, can't really optimize
                            canOptimize = false;
                            break;
                        }
                        canOptimize = true;
                        result = entry.value.queryResult();
                        shardIndex = entry.index;
                    }
                }
            }
            // 只针对单分片的sort
            if (canOptimize) {
                int offset = result.from();
                if (scrollSort) {
                    offset = 0;
                }
                // lucene的TopDocs.scoreDocs
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                if (scoreDocs.length == 0 || scoreDocs.length < offset) {
                    return EMPTY_DOCS;
                }

                // 最终要返回的结果集的size = queryResult().size()
                int resultDocsSize = result.size();
                if ((scoreDocs.length - offset) < resultDocsSize) {
                    resultDocsSize = scoreDocs.length - offset;
                }

                ScoreDoc[] docs = new ScoreDoc[resultDocsSize];

                // 单分片的排序即全局排序
                // 从result.topDocs().scoreDocs获取result.size()大小的数组
                for (int i = 0; i < resultDocsSize; i++) {
                    ScoreDoc scoreDoc = scoreDocs[offset + i];
                    scoreDoc.shardIndex = shardIndex;
                    docs[i] = scoreDoc;
                }
                return docs;
            }
        }

        // 针对命中多分片的docs sort
        // list转array
        @SuppressWarnings("unchecked")
        AtomicArray.Entry<? extends QuerySearchResultProvider>[] sortedResults = results.toArray(new AtomicArray.Entry[results.size()]);

        // 按index字符串对第一阶段结果排序
        Arrays.sort(sortedResults, QUERY_RESULT_ORDERING);

        // 第一个索引分片的结果
        QuerySearchResultProvider firstResult = sortedResults[0].value;

        // 设置排序字段
        final Sort sort;
        if (firstResult.queryResult().topDocs() instanceof TopFieldDocs) {
            TopFieldDocs firstTopDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            // 连续地将排序设置为给定条件: 首先检查第一个SortField, 但是如果它产生平局, 则使用第二个SortField来打破平局等. 最后, 如果在检查所有SortField之后仍然存在平局, 内部的Lucene docid用于打破它
            sort = new Sort(firstTopDocs.fields);
        } else {
            sort = null;
        }

        // 要返回的docs条数
        int topN = firstResult.queryResult().size();

        // Need to use the length of the resultsArr array, since the slots will be based on the position in the resultsArr array
        // 每个分片的topDocs
        // 需要使用resultsArr数组的长度，因为slots将基于resultsArr数组中的位置
        TopDocs[] shardTopDocs = new TopDocs[resultsArr.length()];
        //
        if (firstResult.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            // this is also important since we shortcut and fetch only docs from "from" and up to "size"
            // 如果我们同时进行query和fetch, 我们已经从每个shard中获取了所有文档, 使用它们...
            // 这也很重要, 因为我们快捷地只fetch从"from"到"size"获取文档
            topN *= sortedResults.length; // topN = topN * shard.size
        }
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
            TopDocs topDocs = sortedResult.value.queryResult().topDocs();
            // the 'index' field is the position in the resultsArr atomic array
            shardTopDocs[sortedResult.index] = topDocs;
        }
        int from = firstResult.queryResult().from();
        // 如果是scroll sort, 就从0开始返回
        if (scrollSort) {
            from = 0;
        }
        // TopDocs#merge can't deal with null shard TopDocs
        // 移除掉null shard
        for (int i = 0; i < shardTopDocs.length; i++) {
            if (shardTopDocs[i] == null) {
                shardTopDocs[i] = Lucene.EMPTY_TOP_DOCS;
            }
        }
        // 用 排序字段, 起始位置, topN和每个分片的Docs对数据进行merge, 调用lucene的TopDocs.merge()方法
        TopDocs mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs);
        return mergedTopDocs.scoreDocs;
    }

    public ScoreDoc[] getLastEmittedDocPerShard(SearchRequest request, ScoreDoc[] sortedShardList, int numShards) {
        if (request.scroll() != null) {
            return getLastEmittedDocPerShard(sortedShardList, numShards);
        } else {
            return null;
        }
    }

    public ScoreDoc[] getLastEmittedDocPerShard(ScoreDoc[] sortedShardList, int numShards) {
        ScoreDoc[] lastEmittedDocPerShard = new ScoreDoc[numShards];
        for (ScoreDoc scoreDoc : sortedShardList) {
            lastEmittedDocPerShard[scoreDoc.shardIndex] = scoreDoc;
        }
        return lastEmittedDocPerShard;
    }

    /**
     * Builds an array, with potential null elements, with docs to load.
     */
    public void fillDocIdsToLoad(AtomicArray<IntArrayList> docsIdsToLoad, ScoreDoc[] shardDocs) {
        for (ScoreDoc shardDoc : shardDocs) {
            IntArrayList list = docsIdsToLoad.get(shardDoc.shardIndex);
            if (list == null) {
                list = new IntArrayList(); // can't be shared!, uses unsafe on it later on
                docsIdsToLoad.set(shardDoc.shardIndex, list);
            }
            list.add(shardDoc.doc);
        }
    }

    /**
     * 通用合并Query和Fetch结果, 包括facets hits suggest aggregation
     * @param sortedDocs        已经排序Doc
     * @param queryResultsArr   Query结果
     * @param fetchResultsArr   Fetch结果
     * @return  InternalSearchResponse
     */
    public InternalSearchResponse merge(ScoreDoc[] sortedDocs,
                                        AtomicArray<? extends QuerySearchResultProvider> queryResultsArr,
                                        AtomicArray<? extends FetchSearchResultProvider> fetchResultsArr) {

        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults = queryResultsArr.asList();
        List<? extends AtomicArray.Entry<? extends FetchSearchResultProvider>> fetchResults = fetchResultsArr.asList();

        if (queryResults.isEmpty()) {
            return InternalSearchResponse.empty();
        }

        // 第一阶段Query第一个shard的Query结果
        QuerySearchResult firstResult = queryResults.get(0).value.queryResult();

        boolean sorted = false;
        int sortScoreIndex = -1;
        if (firstResult.topDocs() instanceof TopFieldDocs) {
            // 已经按fields排好序的docs
            sorted = true;
            TopFieldDocs fieldDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            for (int i = 0; i < fieldDocs.fields.length; i++) {
                // 如果是按文档Score进行的排序, 获取最后一个排序字段的下标
                if (fieldDocs.fields[i].getType() == SortField.Type.SCORE) {
                    sortScoreIndex = i;
                }
            }
        }

        // merge facets
        InternalFacets facets = null;
        if (!queryResults.isEmpty()) {
            // we rely on the fact that the order of facets is the same on all query results
            if (firstResult.facets() != null && firstResult.facets().facets() != null && !firstResult.facets().facets().isEmpty()) {
                List<Facet> aggregatedFacets = Lists.newArrayList();
                List<Facet> namedFacets = Lists.newArrayList();
                for (Facet facet : firstResult.facets()) {
                    // aggregate each facet name into a single list, and aggregate it
                    namedFacets.clear();
                    for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                        for (Facet facet1 : entry.value.queryResult().facets()) {
                            if (facet.getName().equals(facet1.getName())) {
                                namedFacets.add(facet1);
                            }
                        }
                    }
                    if (!namedFacets.isEmpty()) {
                        Facet aggregatedFacet = ((InternalFacet) namedFacets.get(0)).reduce(new InternalFacet.ReduceContext(cacheRecycler, namedFacets));
                        aggregatedFacets.add(aggregatedFacet);
                    }
                }
                facets = new InternalFacets(aggregatedFacets);
            }
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        long totalHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut = false;
        Boolean terminatedEarly = null;

        // 遍历query结果的shard, 获取totalHits和MaxScore
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
            QuerySearchResult result = entry.value.queryResult();
            if (result.searchTimedOut()) {
                timedOut = true;
            }
            if (result.terminatedEarly() != null) {
                if (terminatedEarly == null) {
                    terminatedEarly = result.terminatedEarly();
                } else if (result.terminatedEarly()) {
                    terminatedEarly = true;
                }
            }
            totalHits += result.topDocs().totalHits;
            if (!Float.isNaN(result.topDocs().getMaxScore())) {
                maxScore = Math.max(maxScore, result.topDocs().getMaxScore());
            }
        }
        if (Float.isInfinite(maxScore)) {
            maxScore = Float.NaN;
        }

        // clean the fetch counter
        // 清理fetch计数器, 用于从query结果中获取对应下标的doc
        for (AtomicArray.Entry<? extends FetchSearchResultProvider> entry : fetchResults) {
            entry.value.fetchResult().initCounter();
        }

        // 合并Query和Fetch结果集
        // merge hits
        List<InternalSearchHit> hits = new ArrayList<>();
        if (!fetchResults.isEmpty()) {
            // 遍历每一个已经排序的Doc
            for (ScoreDoc shardDoc : sortedDocs) {
                // 获取对应shard的fetch结果
                FetchSearchResultProvider fetchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                if (fetchResultProvider == null) {
                    continue;
                }
                // fetch result 即当前对象
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                // fetch计数器
                int index = fetchResult.counterGetAndIncrement();
                if (index < fetchResult.hits().internalHits().length) {
                    // 获取对应下标已经fetch到的结果
                    InternalSearchHit searchHit = fetchResult.hits().internalHits()[index];
                    // 设置score
                    searchHit.score(shardDoc.score);
                    // 设置shard
                    searchHit.shard(fetchResult.shardTarget());

                    if (sorted) {
                        FieldDoc fieldDoc = (FieldDoc) shardDoc;
                        // 设置排序字段
                        searchHit.sortValues(fieldDoc.fields);
                        if (sortScoreIndex != -1) {
                            // 设置score
                            searchHit.score(((Number) fieldDoc.fields[sortScoreIndex]).floatValue());
                        }
                    }

                    hits.add(searchHit);
                }
            }
        }

        // merge suggest results
        Suggest suggest = null;
        if (!queryResults.isEmpty()) {
            final Map<String, List<Suggest.Suggestion>> groupedSuggestions = new HashMap<>();
            boolean hasSuggestions = false;
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                Suggest shardResult = entry.value.queryResult().queryResult().suggest();

                if (shardResult == null) {
                    continue;
                }
                hasSuggestions = true;
                Suggest.group(groupedSuggestions, shardResult);
            }

            suggest = hasSuggestions ? new Suggest(Suggest.Fields.SUGGEST, Suggest.reduce(groupedSuggestions)) : null;
        }

        // merge addAggregation
        InternalAggregations aggregations = null;
        if (!queryResults.isEmpty()) {
            if (firstResult.aggregations() != null && firstResult.aggregations().asList() != null) {
                List<InternalAggregations> aggregationsList = new ArrayList<>(queryResults.size());
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                    aggregationsList.add((InternalAggregations) entry.value.queryResult().aggregations());
                }
                aggregations = InternalAggregations.reduce(aggregationsList, new ReduceContext(null, bigArrays, scriptService));
            }
        }

        InternalSearchHits searchHits = new InternalSearchHits(hits.toArray(new InternalSearchHit[hits.size()]), totalHits, maxScore);

        return new InternalSearchResponse(searchHits, facets, aggregations, suggest, timedOut, terminatedEarly);
    }

}
