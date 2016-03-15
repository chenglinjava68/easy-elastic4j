package com.github.wens.elastic;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RoutingTest {
    static String test_index = "test_routing";

    private ElasticClient elasticClient;

    @Before
    public void before() throws IOException, InterruptedException {

        elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);
    }

    @Test
    public void test_routing_index() {
        for (int i = 0; i < 10; i++) {
            add_index(String.valueOf(i), "A");
        }

        for (int i = 0; i < 10; i++) {
            add_index(String.valueOf(i), "B");
        }
    }

    @Test
    public void test_routing_query_1() {
        List<Map<String, Object>> list = elasticClient.queryList(new Query(test_index).eq("content", "aaa").routing("A"));
        for (Map<String, Object> map : list) {
            System.out.println("routing A:" + map.get("content"));
        }
        Assert.assertEquals(list.size(), 10);
    }

    @Test
    public void test_routing_query_2() {
        List<Map<String, Object>> list = elasticClient.queryList(new Query(test_index).queryString("content:AAA").routing("A,B"));
        for (Map<String, Object> map : list) {
            System.out.println("routing A,B:" + map.get("content"));
        }
        Assert.assertEquals(list.size(), 20);
    }

    private void add_index(String id, String routing_name) {
        IndexObject indexObject = new IndexObject(id, routing_name);
        indexObject.field("content", "AAA");
        elasticClient.index(test_index, indexObject);
    }
}
