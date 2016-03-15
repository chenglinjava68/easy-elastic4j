package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by wens on 15-10-26.
 */
public class Prefix implements Constraint {
    private final String field;
    private final String value;

    private boolean isFilter;

    private Prefix(String field, String value) {
        this.field = field;
        this.value = value;
    }


    public static Prefix on(String field, String value) {
        return new Prefix(field, value);
    }

    public Prefix asFilter() {
        this.isFilter = true;
        return this;
    }

    @Override
    public QueryBuilder createQuery() {
        if (!this.isFilter) {
            return QueryBuilders.prefixQuery(field, value).rewrite("top_terms_256");
        }
        return null;

    }

    @Override
    public FilterBuilder createFilter() {
        if (this.isFilter) {
            return FilterBuilders.prefixFilter(field, value);
        }
        return null;
    }


    @Override
    public String toString() {
        return field + " STARTS-WITH '" + this.value + "'";
    }
}
