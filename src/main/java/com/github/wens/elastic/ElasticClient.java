package com.github.wens.elastic;

import com.github.wens.elastic.constraints.Completion;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-10-26.
 */
public class ElasticClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticClient.class);

    public static final String ID = "_id";

    public static final String DEFAULT_TYPE = "default";

    protected Client client;

    public ElasticClient(Client c) {
        this.client = c;
    }


    /**
     * Index  associated with a given index
     *
     * @param indexName
     * @param indexObject
     * @return
     */
    public boolean index(String indexName, IndexObject indexObject) {
        return index(indexName, Arrays.asList(indexObject));
    }

    /**
     * Bulk index
     *
     * @param indexName
     * @param indexObjects
     * @return
     */
    public boolean index(String indexName, List<IndexObject> indexObjects) {

        if (indexName == null) {
            throw new NullPointerException("indexName must not be null.");
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (IndexObject indexObject : indexObjects) {
            try {
                bulkRequestBuilder.add(buildIndexRequest(indexName, indexObject));
            } catch (IOException e) {
                throw new ElasticeException(e);
            }
        }

        BulkResponse responses = bulkRequestBuilder.execute().actionGet(10, TimeUnit.SECONDS);
        if (responses.hasFailures()) {
            logger.error("index failed:" + responses.buildFailureMessage());
            return false;
        }
        return true;
    }

    private IndexRequestBuilder buildIndexRequest(String index, IndexObject indexObject) throws IOException {
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, DEFAULT_TYPE);
        if (!Strings.isEmpty(indexObject.id)) {
            indexRequestBuilder.setId(indexObject.id);
        }
        if (!Strings.isEmpty(indexObject.routing)) {
            indexRequestBuilder.setRouting(indexObject.routing);
        }

        if (indexObject.ttl != -1) {
            indexRequestBuilder.setTTL(indexObject.ttl);
        }

        return indexRequestBuilder.setSource(indexObject.build());
    }

    /**
     * Update
     *
     * @param indexName
     * @param indexObject
     * @return
     */
    public boolean update(String indexName, IndexObject indexObject) {
        return update(indexName, Arrays.asList(indexObject));
    }

    /**
     * Bulk update
     *
     * @param indexName
     * @param indexObjects
     * @return
     */
    public boolean update(String indexName, List<IndexObject> indexObjects) {
        if (indexName == null) {
            throw new NullPointerException("indexName must not be null.");
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (IndexObject indexObject : indexObjects) {
            try {
                bulkRequestBuilder.add(buildUpdateRequest(indexName, indexObject));
            } catch (IOException e) {
                throw new ElasticeException(e);
            }
        }

        BulkResponse responses = bulkRequestBuilder.execute().actionGet(10, TimeUnit.SECONDS);
        if (responses.hasFailures()) {
            logger.error("update index failed:" + responses.buildFailureMessage());
            return false;
        }
        return true;
    }

    private UpdateRequestBuilder buildUpdateRequest(String index, IndexObject indexObject) throws IOException {
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, "default", indexObject.id);
        try {
            if (Strings.isEmpty(indexObject.id)) {
                throw new IllegalArgumentException("id must be set when using update");
            }
            if (!Strings.isEmpty(indexObject.routing)) {
                updateRequestBuilder.setRouting(indexObject.routing);
            }
            return updateRequestBuilder.setDoc(indexObject.build());
        } catch (Exception e) {
            throw new ElasticeException(e);
        }
    }

    /**
     * Delete base on id
     *
     * @param indexName
     * @param id
     * @return
     */
    public boolean delete(String indexName, String id) {
        return delete(indexName, Arrays.asList(id));
    }

    /**
     * Bulk delete base on id
     *
     * @param indexName
     * @param ids
     * @return
     */
    public boolean delete(String indexName, List<String> ids) {
        if (indexName == null) {
            throw new NullPointerException("indexName must not be null.");
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (String id : ids) {
            bulkRequestBuilder.add(client.prepareDelete(indexName, "default", id));
        }

        BulkResponse responses = bulkRequestBuilder.execute().actionGet(10, TimeUnit.SECONDS);
        if (responses.hasFailures()) {
            logger.error("update index failed:" + responses.buildFailureMessage());
            return false;
        }
        return true;
    }

    /**
     * Count base on query's condition
     *
     * @param query
     * @return
     */
    public long count(Query query) {
        try {
            CountRequestBuilder crb = client.prepareCount(query.index);
            QueryBuilder qb = query.buildQuery();
            if (qb != null) {
                crb.setQuery(qb);
            }
            FilterBuilder fb = query.buildFilter();
            if (fb != null) {
                crb.setQuery(QueryBuilders.filteredQuery(qb == null ? QueryBuilders.matchAllQuery() : qb, fb));
            }
            return transformCount(crb);
        } catch (Throwable t) {
            throw new ElasticeException(t);
        }
    }

    private long transformCount(CountRequestBuilder builder) {
        CountResponse res = builder.execute().actionGet();
        return res.getCount();
    }

    /**
     * Query for first one
     *
     * @param query
     * @return
     */
    public Map<String, Object> queryFirst(Query query) {
        try {

            SearchRequestBuilder srb = query.buildSearch(client);
            return transformFirst(srb);
        } catch (Throwable e) {
            throw new ElasticeException(e);
        }
    }

    /**
     * query for a list
     *
     * @param query
     * @return
     */
    public List<Map<String, Object>> queryList(Query query) {
        return queryResultList(query);
    }


    private List<Map<String, Object>> queryResultList(Query query) {
        try {

            if (query.limit == null) {
                query.limit = Query.DEFAULT_LIMIT;
            }

            SearchRequestBuilder srb = query.buildSearch(client);
            return transform(srb);
        } catch (Throwable e) {
            throw new ElasticeException(e);
        }
    }

    /**
     * Query for page
     *
     * @param query
     * @return
     */
    public Page queryPage(Query query) {
        if (query.limit == null) {
            throw new IllegalStateException("limit must be set when using queryPage");
        }
        int originalLimit = query.limit;
        query.limit++;
        List<Map<String, Object>> result = queryResultList(query);
        boolean hasMore = false;
        if (result.size() > originalLimit) {
            hasMore = true;
            result.remove(result.size() - 1);
        }
        return new Page().start(query.start + 1)
                .items(result)
                .more(hasMore)
                .pageSize(originalLimit);
    }

    protected List<Map<String, Object>> transform(SearchRequestBuilder builder) throws Exception {
        SearchResponse searchResponse = builder.execute().actionGet();
        List<Map<String, Object>> list = new ArrayList<>((int) searchResponse.getHits().getTotalHits());

        for (SearchHit hit : searchResponse.getHits()) {
            Map<String, Object> source = hit.getSource();
            source.put(ElasticClient.ID, hit.getId());
            list.add(source);
        }
        return list;
    }


    protected Map<String, Object> transformFirst(SearchRequestBuilder builder) throws Exception {
        SearchResponse searchResponse = builder.execute().actionGet();
        Map<String, Object> result = null;
        if (searchResponse.getHits().hits().length > 0) {
            SearchHit hit = searchResponse.getHits().hits()[0];
            result = hit.getSource();
            result.put(ElasticClient.ID, hit.id());
        }
        return result;
    }

    public Aggregations queryAggregations(Query query) {
        try {
            SearchRequestBuilder srb = query.buildSearch(client);
            SearchResponse sr = srb.execute().actionGet();
            return sr.getAggregations();
        } catch (Throwable e) {
            throw new ElasticeException(e);
        }
    }

    public List<String> queryCompletion(String index, Completion completion) {
        SuggestResponse srb = Completion.buildSuggest(index, client);

        List<? extends Entry<? extends Option>> list = srb.getSuggest().getSuggestion(Completion.SUGGESTION_NAME).getEntries();
        List<String> suggests = new ArrayList<String>();
        if (list != null) {
            for (Entry<? extends Option> e : list) {
                for (Option option : e) {
                    suggests.add(option.getText().toString());
                }
            }
        }
        return suggests;
    }

}
