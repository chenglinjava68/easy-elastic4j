package com.github.wens.elastic;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by wens on 15-10-26.
 */
public class ElasticClientTest {

    static String test_index = "test_search";

    private ElasticClient elasticClient;

    @Before
    public void before() throws IOException, InterruptedException {
        elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);
    }

    @After
    public void after() {
        elasticClient.client.admin().indices().prepareDelete("test_search").execute().actionGet();
    }


    @Test
    public void test_index_1() throws InterruptedException {
        IndexObject indexObject = new IndexObject("1");

        indexObject.field("name", "wens");
        indexObject.field("age", 29);
        indexObject.field("height", 165.5f);
        indexObject.field("comment", "攻城狮");

        boolean test_search = elasticClient.index(test_index, indexObject);
        Assert.assertEquals(true, test_search);

        Thread.sleep(1000);

        Assert.assertEquals(1L, elasticClient.count(new Query(test_index)));
    }

    @Test
    public void test_index_2() throws InterruptedException {
        List<IndexObject> list = new ArrayList<>();
        Random r = new Random();
        int count = r.nextInt(100);
        for (int i = 0; i < count; i++) {
            IndexObject indexObject = new IndexObject(String.valueOf(i));
            indexObject.field("name", "wens" + i);
            indexObject.field("age", r.nextInt(30));
            indexObject.field("height", 165 + r.nextFloat());
            indexObject.field("comment", "攻城狮" + i);
            list.add(indexObject);
        }

        boolean test_search = elasticClient.index(test_index, list);
        Assert.assertEquals(true, test_search);

        Thread.sleep(5000);

        Assert.assertEquals((long) count, elasticClient.count(new Query(test_index)));
    }

    @Test
    public void test_update_1() throws InterruptedException {
        test_index_1();
        IndexObject indexObject = new IndexObject("1");

        indexObject.field("comment", "攻城狮 1024");

        boolean test_search = elasticClient.update(test_index, indexObject);
        Assert.assertEquals(true, test_search);

        Thread.sleep(1000);

        Assert.assertEquals(1L, elasticClient.count(new Query(test_index)));
    }

    @Test
    public void test_delete_1() throws InterruptedException {
        test_index_1();

        elasticClient.delete(test_index, "1");

        Thread.sleep(1000);

        Assert.assertEquals(0L, elasticClient.count(new Query(test_index)));
    }


    public void index(int num) throws InterruptedException {
        List<IndexObject> list = new ArrayList<>();
        Random r = new Random();
        for (int i = 0; i < num; i++) {
            IndexObject indexObject = new IndexObject(String.valueOf(i));
            indexObject.field("name", String.format("wens%05d", i));
            indexObject.field("group", "coder");
            indexObject.field("age", r.nextInt(30));
            indexObject.field("height", 165 + r.nextFloat());
            indexObject.field("comment", "攻城狮" + i);
            list.add(indexObject);
        }

        boolean test_search = elasticClient.index(test_index, list);
        Assert.assertEquals(true, test_search);

        Thread.sleep(5000);
    }


    @Test
    public void test_queryFirst() throws InterruptedException {
        index(1000);

        Map<String, Object> item = elasticClient.queryFirst(new Query(test_index).eq("group", "coder"));
        Assert.assertNotNull(item);
        Assert.assertNotNull(item.get("_id"));
        Assert.assertNotNull(item.get("name"));
        Assert.assertNotNull(item.get("group"));
        Assert.assertNotNull(item.get("age"));
        Assert.assertNotNull(item.get("height"));
        Assert.assertNotNull(item.get("comment"));

    }

    @Test
    public void test_queryList() throws InterruptedException {
        index(1000);

        List<Map<String, Object>> list = elasticClient.queryList(new Query(test_index).eq("group", "coder").limit(0, 5).orderByAsc("name"));

        Assert.assertEquals(5, list.size());
        int id = 0;
        for (Map<String, Object> item : list) {
            Assert.assertEquals(String.format("wens%05d", id++), item.get("name"));
        }

    }


    @Test
    public void test_queryPage() throws InterruptedException {
        index(10);

        Page page = elasticClient.queryPage(new Query(test_index).eq("group", "coder").limit(0, 5).orderByAsc("name"));

        Assert.assertEquals(5, page.getItems().size());
        Assert.assertEquals(true, page.isMore());
        int id = 0;
        for (Map<String, Object> item : page.getItems()) {
            Assert.assertEquals(String.format("wens%05d", id++), item.get("name"));
        }


        page = elasticClient.queryPage(new Query(test_index).eq("group", "coder").limit(5, 5).orderByAsc("name"));

        Assert.assertEquals(5, page.getItems().size());
        Assert.assertEquals(false, page.isMore());
        id = 5;
        for (Map<String, Object> item : page.getItems()) {
            Assert.assertEquals(String.format("wens%05d", id++), item.get("name"));
        }

    }



}
