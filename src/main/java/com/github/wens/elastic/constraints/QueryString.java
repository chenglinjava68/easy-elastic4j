package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by wens on 15-10-26.
 */
public class QueryString implements Constraint {

    private final String value;

    private QueryString(String value) {
        this.value = value;
    }


    public static QueryString query(String value) {
        return new QueryString(value);
    }

    @Override
    public QueryBuilder createQuery() {
        return QueryBuilders.queryStringQuery(this.value);
    }

    @Override
    public FilterBuilder createFilter() {
        return null;
    }


    @Override
    public String toString() {
        return "_QUERY" + " = '" + this.value + "'";
    }
}
