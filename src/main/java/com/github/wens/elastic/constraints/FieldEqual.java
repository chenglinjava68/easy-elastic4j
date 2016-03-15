package com.github.wens.elastic.constraints;

import com.github.wens.elastic.ElasticClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by wens on 15-10-26.
 */
public class FieldEqual implements Constraint {
    private final String field;
    private final Object value;
    private boolean isFilter;

    private FieldEqual(String field, Object value) {
        if (ElasticClient.ID.equalsIgnoreCase(field)) {
            this.field = ElasticClient.ID;
        } else {
            this.field = field;
        }
        this.value = value;
    }


    public static FieldEqual on(String field, Object value) {
        return new FieldEqual(field, value);
    }

    public FieldEqual asFilter() {
        this.isFilter = true;
        return this;
    }

    @Override
    public QueryBuilder createQuery() {
        if (this.value == null) {
            return null;
        }

        if (!isFilter) {
            return QueryBuilders.termQuery(this.field, this.value);
        }

        return null;
    }

    @Override
    public FilterBuilder createFilter() {
        if (this.value == null) {
            return null;
        }
        if (isFilter) {
            return FilterBuilders.termFilter(this.field, this.value);
        }
        return null;

    }

    @Override
    public String toString() {
        return this.field + " = '" + this.value + "'";
    }
}
