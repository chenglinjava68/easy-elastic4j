package com.github.wens.elastic.constraints;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by wens on 15-11-3.
 */
public class Nearby implements Constraint {

    public final String field;
    public final double lat;
    public final double lon;
    public final double distance;


    private Nearby(String field, double lat, double lon, double distance) {
        this.field = field;
        this.lat = lat;
        this.lon = lon;
        this.distance = distance;
    }

    public static Nearby nearby(String field, double lat, double lon, double distance) {
        return new Nearby(field, lat, lon, distance);
    }

    @Override
    public QueryBuilder createQuery() {
        return null;
    }

    @Override
    public FilterBuilder createFilter() {
        return FilterBuilders
                .geoDistanceFilter(field)
                .point(lat, lon)
                .distance(distance, DistanceUnit.METERS)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);
    }
}
