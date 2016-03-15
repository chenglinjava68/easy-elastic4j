package com.github.wens.elastic.constraints;

import com.github.wens.elastic.ElasticClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by wens on 15-10-26.
 */
public class FieldExist implements Constraint {
    private final String field;
    private boolean isFilter;

    private FieldExist(String field) {
        if (ElasticClient.ID.equalsIgnoreCase(field)) {
            this.field = ElasticClient.ID;
        } else {
            this.field = field;
        }
        isFilter = true;
    }


    public static FieldExist on(String field) {
        return new FieldExist(field);
    }

    public FieldExist asFilter() {
        this.isFilter = true;
        return this;
    }

    @Override
    public QueryBuilder createQuery() {

        return null;
    }

    @Override
    public FilterBuilder createFilter() {

        if (isFilter) {
            return FilterBuilders.existsFilter(this.field);
        }

        return null;
    }

    @Override
    public String toString() {
        return this.field + " is not null";
    }
}
