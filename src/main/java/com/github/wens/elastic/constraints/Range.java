package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.*;

/**
 * Created by wens on 15-10-26.
 */
public class Range implements Constraint {

    private enum Bound {
        LT, LT_EQ, GT, GT_EQ
    }

    private String field;
    private Object value;
    private Bound bound;
    private boolean isFilter;


    private Range(String field, Bound bound, Object value) {
        this.field = field;
        this.value = value;
        this.bound = bound;
    }


    public static Range less(String field, Object value) {
        return new Range(field, Bound.LT, value);
    }


    public static Range greater(String field, Object value) {
        return new Range(field, Bound.GT, value);
    }

    public static Range lessEqual(String field, Object value) {
        return new Range(field, Bound.LT_EQ, value);
    }


    public static Range greaterEqual(String field, Object value) {
        return new Range(field, Bound.GT_EQ, value);
    }

    public Range asFilter() {
        this.isFilter = true;
        return this;
    }


    public Range including() {
        if (bound == Bound.LT) {
            bound = Bound.LT_EQ;
        } else if (bound == Bound.GT) {
            bound = Bound.GT_EQ;
        }

        return this;
    }


    @Override
    public QueryBuilder createQuery() {
        if (!this.isFilter) {
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
            if (bound == Bound.LT) {
                rangeQueryBuilder.lt(value);
            } else if (bound == Bound.LT_EQ) {
                rangeQueryBuilder.lte(value);
            } else if (bound == Bound.GT_EQ) {
                rangeQueryBuilder.gte(value);
            } else if (bound == Bound.GT) {
                rangeQueryBuilder.gt(value);
            }

            return rangeQueryBuilder;
        }
        return null;
    }

    @Override
    public FilterBuilder createFilter() {
        if (this.isFilter) {
            RangeFilterBuilder rangeFilterBuilder = FilterBuilders.rangeFilter(field);
            if (bound == Bound.LT) {
                rangeFilterBuilder.lt(value);
            } else if (bound == Bound.LT_EQ) {
                rangeFilterBuilder.lte(value);
            } else if (bound == Bound.GT_EQ) {
                rangeFilterBuilder.gte(value);
            } else if (bound == Bound.GT) {
                rangeFilterBuilder.gt(value);
            }

            return rangeFilterBuilder;
        }
        return null;
    }

    @Override
    public String toString() {
        return field + " " + bound + " '" + this.value + "'";
    }
}
