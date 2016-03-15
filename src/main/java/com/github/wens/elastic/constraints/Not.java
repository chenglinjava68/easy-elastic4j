package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.*;

/**
 * Created by wens on 15-10-26.
 */
public class Not implements Constraint {

    private Constraint inner;


    private Not(Constraint inner) {
        this.inner = inner;
    }


    public static Constraint on(Constraint inner) {
        return new Not(inner);
    }

    @Override
    public QueryBuilder createQuery() {
        QueryBuilder innerQuery = inner.createQuery();
        if (innerQuery == null) {
            return null;
        }
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.mustNot(innerQuery);
        return qb;
    }

    @Override
    public FilterBuilder createFilter() {
        FilterBuilder innerFilter = inner.createFilter();
        if (innerFilter == null) {
            return null;
        }
        BoolFilterBuilder qb = FilterBuilders.boolFilter();
        qb.mustNot(innerFilter);
        return qb;
    }


    @Override
    public String toString() {
        return "!" + inner.toString();
    }
}
