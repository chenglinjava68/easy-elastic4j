package com.github.wens.elastic;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by wens on 15-10-26.
 */
public class IndexObject {

    protected XContentBuilder builder;

    protected String id;

    protected String routing;

    protected long ttl = -1;

    public IndexObject() {
        this(null);
    }

    public IndexObject(String id, String routing) {
        this(id, -1, routing);
    }

    public IndexObject(String id, long ttl) {
        this(id, -1, null);
    }

    public IndexObject(String id, long ttl, String routing) {
        this.id = id;
        this.routing = routing;
        this.ttl = ttl;
        try {
            builder = jsonBuilder().startObject();
        } catch (IOException e) {
            throw new ElasticeException(e);
        }
    }

    public IndexObject(String id) {
        this.id = id;
        try {
            builder = jsonBuilder().startObject();
        } catch (IOException e) {
            throw new ElasticeException(e);
        }
    }

    public IndexObject id(String value) {
        this.id = value;
        return this;
    }

    public IndexObject field(String name, Object value) {

        if (ElasticClient.ID.equalsIgnoreCase(name)) {
            this.id = Strings.toString(value);

        } else {
            try {
                builder.field(name, value);
            } catch (IOException e) {
                throw new ElasticeException(e);
            }
        }
        return this;
    }

    protected XContentBuilder build() {
        try {
            return builder.endObject();
        } catch (IOException e) {
            throw new ElasticeException(e);
        }
    }


}
