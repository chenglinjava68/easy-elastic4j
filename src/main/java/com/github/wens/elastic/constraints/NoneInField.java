package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.*;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by wens on 15-10-26.
 */
public class NoneInField implements Constraint {

    private final Collection<?> values;
    private final String field;
    private boolean isFilter;

    private NoneInField(Collection<?> values, String field) {
        this.values = values;
        this.field = field;
    }


    public static NoneInField on(Collection<?> values, String field) {
        return new NoneInField(values, field);
    }

    public NoneInField asFilter() {
        this.isFilter = true;
        return this;
    }

    @Override
    public QueryBuilder createQuery() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (!this.isFilter) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Object value : values) {
                boolQueryBuilder.mustNot(FieldEqual.on(this.field, value).createQuery());
            }
            return boolQueryBuilder;
        }
        return null;
    }

    @Override
    public FilterBuilder createFilter() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (this.isFilter) {
            BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
            for (Object value : values) {
                boolFilterBuilder.mustNot(FieldEqual.on(this.field, value).asFilter().createFilter());
            }
            return boolFilterBuilder;
        }
        return null;
    }


    @Override
    public String toString() {
        if (values == null || values.isEmpty()) {
            return "<skipped>";
        }
        return "(" + Arrays.toString(this.values.toArray()) + " NOT IN " + this.field;
    }
}
