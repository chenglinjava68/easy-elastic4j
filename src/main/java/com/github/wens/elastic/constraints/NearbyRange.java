package com.github.wens.elastic.constraints;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by wens on 15-11-3.
 */
public class NearbyRange implements Constraint {

    public final String field;
    public final double lat;
    public final double lon;
    public final double from;
    public final double to;


    private NearbyRange(String field, double lat, double lon, double from, double to) {
        this.field = field;
        this.lat = lat;
        this.lon = lon;
        this.from = from;
        this.to = to;
    }

    public static NearbyRange nearbyRange(String field, double lat, double lon, double from, double to) {
        return new NearbyRange(field, lat, lon, from, to);
    }

    @Override
    public QueryBuilder createQuery() {
        return null;
    }

    @Override
    public FilterBuilder createFilter() {
        return FilterBuilders
                .geoDistanceRangeFilter(field)
                .point(lat, lon)
                .from(from)
                .to(to)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);
    }
}
