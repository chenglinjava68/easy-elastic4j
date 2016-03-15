package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.*;

/**
 * Created by wens on 15-10-26.
 */
public class And implements Constraint {

    private Constraint[] constraints;

    private And(Constraint... constraints) {
        this.constraints = constraints;
    }


    public static Constraint on(Constraint... constraints) {
        return new And(constraints);
    }

    @Override
    public QueryBuilder createQuery() {
        BoolQueryBuilder result = QueryBuilders.boolQuery();
        boolean found = false;
        for (Constraint constraint : constraints) {
            QueryBuilder qb = constraint.createQuery();
            if (qb != null) {
                found = true;
                result.must(qb);
            }
        }
        if (!result.hasClauses()) {
            return null;
        }
        return found ? result : null;
    }

    @Override
    public FilterBuilder createFilter() {

        BoolFilterBuilder result = FilterBuilders.boolFilter();
        boolean found = false;
        for (Constraint constraint : constraints) {
            FilterBuilder qb = constraint.createFilter();
            if (qb != null) {
                found = true;
                result.must(qb);
            }
        }
        if (!result.hasClauses()) {
            return null;
        }
        return found ? result : null;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (Constraint child : constraints) {
            if (sb.length() > 1) {
                sb.append(") AND (");
            }
            sb.append(child.toString());
        }
        sb.append(")");
        return sb.toString();
    }
}
