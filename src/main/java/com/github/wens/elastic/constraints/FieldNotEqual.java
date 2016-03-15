package com.github.wens.elastic.constraints;

import com.github.wens.elastic.ElasticClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by wens on 15-10-26.
 */
public class FieldNotEqual implements Constraint {
    private final String field;
    private final Object value;

    private boolean isFilter;

    private FieldNotEqual(String field, Object value) {
        if (ElasticClient.ID.equalsIgnoreCase(field)) {
            this.field = ElasticClient.ID;
        } else {
            this.field = field;
        }
        this.value = value;
    }


    public static FieldNotEqual on(String field, Object value) {
        return new FieldNotEqual(field, value);
    }

    public FieldNotEqual asFilter() {
        this.isFilter = true;
        return this;
    }

    @Override
    public QueryBuilder createQuery() {

        if (!this.isFilter) {
            return Not.on(FieldEqual.on(this.field, this.value)).createQuery();
        }
        return null;
    }

    @Override
    public FilterBuilder createFilter() {

        if (this.isFilter) {
            return Not.on(FieldEqual.on(this.field, this.value).asFilter()).createFilter();
        }

        return null;
    }


    @Override
    public String toString() {
        return this.field + " != '" + this.value + "'";
    }
}
