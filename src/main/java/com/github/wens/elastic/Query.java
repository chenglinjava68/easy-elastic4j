package com.github.wens.elastic;

import com.github.wens.elastic.constraints.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by wens on 15-10-22.
 */
public class Query {

    protected static int DEFAULT_LIMIT = 25;

    protected List<Constraint> constraints = new ArrayList<>();
    protected List<Tuple<String, Boolean>> orderBys = new ArrayList<>();
    protected int start;
    protected Integer limit = null;
    protected String query;
    protected String index;
    protected String type;
    protected String routing;

    public Query() {
    }

    public Query(String indexName) {
        this(indexName, ElasticClient.DEFAULT_TYPE);
    }

    public Query(String indexName, String type) {
        if (Strings.isEmpty(indexName)) {
            throw new NullPointerException("indexName must not be empty.");
        }
        this.index = indexName;
        this.type = type;
    }

    public Query where(Constraint... constraints) {
        this.constraints.addAll(Arrays.asList(constraints));
        return this;
    }


    public Query or(Constraint... constraints) {
        this.constraints.add(Or.on(constraints));
        return this;
    }

    public Query eq(String field, Object value) {
        constraints.add(FieldEqual.on(field, value));
        return this;
    }

    public Query exist(String field) {
        constraints.add(FieldExist.on(field));
        return this;
    }

    public Query eqIgnoreNull(String field, Object value) {
        if (Strings.isFilled(value)) {
            eq(field, value);
        }
        return this;
    }


    public Query notEq(String field, Object value) {
        constraints.add(FieldNotEqual.on(field, value));
        return this;
    }

    public Query in(String field, Collection<?> values) {
        constraints.add(OneInField.on(values, field));
        return this;
    }

    public Query notIn(String field, Collection<?> values) {
        constraints.add(NoneInField.on(values, field));
        return this;
    }

    public Query gt(String field, Object value) {
        constraints.add(Range.greater(field, value));
        return this;
    }

    public Query gtEq(String field, Object value) {
        constraints.add(Range.greaterEqual(field, value));
        return this;
    }

    public Query lt(String field, Object value) {
        constraints.add(Range.less(field, value));
        return this;
    }

    public Query ltEq(String field, Object value) {
        constraints.add(Range.lessEqual(field, value));
        return this;
    }

    public Query prefix(String field, String value) {
        constraints.add(Prefix.on(field, value));
        return this;
    }

    public Query nearby(String field, double lat, double lon, double distance) {
        constraints.add(Nearby.nearby(field, lat, lon, distance));
        return this;
    }

    public Query nearbyRange(String field, double lat, double lon, double form, double to) {
        constraints.add(NearbyRange.nearbyRange(field, lat, lon, form, to));
        return this;
    }


    public Query queryString(String queryString) {
        constraints.add(QueryString.query(queryString));
        return this;
    }


    public Query orderByAsc(String field) {
        orderBys.add(new Tuple<>(field, true));
        return this;
    }


    public Query orderByDesc(String field) {
        orderBys.add(new Tuple<>(field, false));
        return this;
    }


    private Query start(int start) {
        this.start = Math.max(start, 0);
        return this;
    }


    private Query limit(int limit) {
        this.limit = Math.max(0, limit);
        return this;
    }

    public Query limit(int start, int limit) {
        return start(start).limit(limit);
    }

    public Query routing(String routing) {
        this.routing = routing;
        return this;
    }


    protected SearchRequestBuilder buildSearch(Client client) {
        SearchRequestBuilder srb = client.prepareSearch(index).setTypes(type);
        srb.setVersion(true);
        applyOrderBys(srb);
        applyQueriesAndFilters(srb);
        applyLimit(srb);
        applyRouting(srb);
        return srb;
    }

    private void applyRouting(SearchRequestBuilder srb) {
        if (StringUtils.isNotEmpty(routing)) {
            srb.setRouting(routing);
        }
    }


    private void applyLimit(SearchRequestBuilder srb) {
        if (start > 0) {
            srb.setFrom(start);
        }
        if (limit != null && limit > 0) {
            srb.setSize(limit);
        }
    }

    private void applyOrderBys(SearchRequestBuilder srb) {
        for (Tuple<String, Boolean> sort : orderBys) {
            srb.addSort(sort.getOne(), sort.getTwo() ? SortOrder.ASC : SortOrder.DESC);
        }

        for (Constraint constraint : constraints) {
            FilterBuilder sb = constraint.createFilter();
            if (sb != null && constraint instanceof Nearby) {
                Nearby n = (Nearby) constraint;

                GeoDistanceSortBuilder sort = new GeoDistanceSortBuilder(n.field);
                sort.unit(DistanceUnit.METERS);
                sort.order(SortOrder.ASC);
                sort.point(n.lat, n.lon);
                sort.geoDistance(GeoDistance.ARC);
                srb.addSort(sort);
            }

            if (sb != null && constraint instanceof NearbyRange) {
                NearbyRange n = (NearbyRange) constraint;

                GeoDistanceSortBuilder sort = new GeoDistanceSortBuilder(n.field);
                sort.unit(DistanceUnit.METERS);
                sort.order(SortOrder.ASC);
                sort.point(n.lat, n.lon);
                sort.geoDistance(GeoDistance.ARC);
                srb.addSort(sort);
            }
        }
    }

    private void applyQueriesAndFilters(SearchRequestBuilder srb) {
        QueryBuilder qb = buildQuery();
        if (qb != null) {
            srb.setQuery(qb);
        }
        FilterBuilder fb = buildFilter();

        if (fb != null) {
            srb.setPostFilter(fb);
        }
    }


    protected QueryBuilder buildQuery() {
        List<QueryBuilder> queries = new ArrayList<QueryBuilder>();
        for (Constraint constraint : constraints) {
            QueryBuilder qb = constraint.createQuery();
            if (qb != null) {
                queries.add(qb);
            }
        }
        if (queries.isEmpty()) {
            return null;
        } else if (queries.size() == 1) {
            return queries.get(0);
        } else {
            BoolQueryBuilder result = QueryBuilders.boolQuery();
            for (QueryBuilder qb : queries) {
                result.must(qb);
            }
            return result;
        }
    }

    protected FilterBuilder buildFilter() {
        List<FilterBuilder> filters = new ArrayList<FilterBuilder>();
        for (Constraint constraint : constraints) {
            FilterBuilder sb = constraint.createFilter();
            if (sb != null) {
                filters.add(sb);
            }
        }
        if (filters.isEmpty()) {
            return null;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            BoolFilterBuilder result = FilterBuilders.boolFilter();
            for (FilterBuilder qb : filters) {
                result.must(qb);
            }
            return result;
        }
    }


    public String toString(boolean skipConstraintValues) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(this.index);
        if (!constraints.isEmpty()) {
            sb.append(" WHERE ");
            boolean first = true;
            for (Constraint constraint : constraints) {
                if (!first) {
                    sb.append(" AND ");
                }
                first = false;
                sb.append(constraint.toString());
            }
        }
        if (!orderBys.isEmpty()) {
            sb.append(" ORDER BY");
            for (Tuple<String, Boolean> orderBy : orderBys) {
                sb.append(" ");
                sb.append(orderBy.getOne());
                sb.append(orderBy.getTwo() ? " ASC" : " DESC");
            }
        }
        if (start > 0 || (limit != null && limit > 0)) {
            sb.append(" LIMIT ");
            sb.append(skipConstraintValues ? "?" : start);
            sb.append(", ");
            sb.append(skipConstraintValues ? "?" : limit);
        }
        return sb.toString();
    }


    @Override
    public String toString() {
        return toString(false);
    }


}
