package com.github.wens.elastic.constraints;

import com.github.wens.elastic.ElasticClient;
import org.elasticsearch.index.query.*;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by wens on 15-10-26.
 */
public class OneInField implements Constraint {

    private final Collection<?> values;
    private final String field;

    private boolean isFilter;

    private OneInField(Collection<?> values, String field) {
        if (values != null) {
            this.values = values;
        } else {
            this.values = null;
        }
        if (ElasticClient.ID.equalsIgnoreCase(field)) {
            this.field = ElasticClient.ID;
        } else {
            this.field = field;
        }
    }

    public static OneInField on(Collection<?> values, String field) {
        return new OneInField(values, field);
    }

    public OneInField asFilter() {
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
                boolQueryBuilder.should(FieldEqual.on(this.field, value).createQuery());
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
                boolFilterBuilder.should(FieldEqual.on(this.field, value).asFilter().createFilter());
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
        return "(" + Arrays.toString(this.values.toArray()) + " IN " + this.field;
    }
}
