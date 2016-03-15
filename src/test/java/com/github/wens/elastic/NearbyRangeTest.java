package com.github.wens.elastic;

import junit.framework.Assert;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NearbyRangeTest {


    static final String query_geo = "query_geo";

    private ElasticClient elasticClient;

    @Before
    public void before() throws IOException, InterruptedException {

        elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("default")
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        elasticClient.client.admin().indices().prepareCreate(query_geo).addMapping("default", mapping).execute().actionGet();

        InputStream inputStream = QueryTest.class.getResourceAsStream("/geo_data.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;

        List<IndexObject> list = new ArrayList<>();

        int id = 0;

        while ((line = bufferedReader.readLine()) != null) {

            String[] items = line.split(" ");
            IndexObject indexObject = new IndexObject(String.valueOf(id++));
            indexObject.field("name", items[0]);
            indexObject.field("location", items[2] + "," + items[1]);
            indexObject.field("num", Integer.parseInt(items[3]));
            list.add(indexObject);
        }

        elasticClient.index(query_geo, list);
        inputStream.close();

        Thread.sleep(5000);
    }

    @After
    public void after() {
        elasticClient.client.admin().indices().prepareDelete(query_geo).execute().actionGet();
    }


    @Test
    public void test_1() {

        //西门口附近
        double lat = 23.131187;
        double lon = 113.257174;
        Query query = new Query(query_geo);
        query.nearbyRange("location", lat, lon, 5000, 10000);
        List<Map<String, Object>> list = elasticClient.queryList(query);
        double last = 0;
        for (Map<String, Object> item : list) {
            String location = (String) item.get("location");
            String[] point = location.split(",");
            double distance = GeoUtils.getDistance(lat, lon, Double.parseDouble(point[0]), Double.parseDouble(point[1]));
            Assert.assertEquals(true, last <= distance);
        }

    }
}
