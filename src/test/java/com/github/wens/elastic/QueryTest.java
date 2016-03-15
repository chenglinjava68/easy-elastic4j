package com.github.wens.elastic;

import com.github.wens.elastic.constraints.And;
import com.github.wens.elastic.constraints.FieldEqual;
import com.github.wens.elastic.constraints.Or;
import com.github.wens.elastic.constraints.Range;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wens on 15-10-27.
 */
public class QueryTest {

    static final String query_test = "query_geo";

    private ElasticClient elasticClient;

    @Before
    public void before() throws IOException, InterruptedException {

        elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);

        InputStream inputStream = QueryTest.class.getResourceAsStream("/test_data.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;

        List<IndexObject> list = new ArrayList<>();

        int id = 0;

        while ((line = bufferedReader.readLine()) != null) {

            String[] items = line.split("\\|\\|");

            IndexObject indexObject = new IndexObject(String.valueOf(id++));
            indexObject.field("content", items[0]);
            indexObject.field("date", items[1]);
            indexObject.field("star", Integer.parseInt(items[2]));
            indexObject.field("score", Float.parseFloat(items[3]));
            list.add(indexObject);
        }

        elasticClient.index(query_test, list);
        inputStream.close();

        Thread.sleep(5000);
    }

    @After
    public void after() {
        elasticClient.client.admin().indices().prepareDelete(query_test).execute().actionGet();
    }


    @Test
    public void test_field_equal() {

        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).eq("_id", 1));

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("网易邮箱惊现致命漏洞，似邮箱过亿数据泄漏", list.get(0).get("content"));
        Assert.assertEquals("2015-10-19", list.get(0).get("date"));
        Assert.assertEquals(122, list.get(0).get("star"));
        Assert.assertEquals(0.8, list.get(0).get("score"));

    }


    @Test
    public void test_field_not_equal() {

        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).notEq("_id", 1));

        Assert.assertEquals(9, list.size());

    }

    @Test
    public void test_range() {

        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).gt("star", 100));

        Assert.assertEquals(3, list.size());

        list = elasticClient.queryList(new Query(query_test).gtEq("star", 116));

        Assert.assertEquals(3, list.size());

        list = elasticClient.queryList(new Query(query_test).lt("star", 100));

        Assert.assertEquals(7, list.size());

        list = elasticClient.queryList(new Query(query_test).ltEq("star", 116));

        Assert.assertEquals(8, list.size());

        list = elasticClient.queryList(new Query(query_test).gt("score", 0.5));

        Assert.assertEquals(4, list.size());

    }


    @Test
    public void test_one_in_field() {

        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).in("star", Arrays.asList(116, 91)));

        Assert.assertEquals(2, list.size());

    }

    @Test
    public void test_none_in_field() {

        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).notIn("star", Arrays.asList(116, 91)));

        Assert.assertEquals(8, list.size());

    }

    @Test
    public void test_prefix() {

        //List<Map<String, Object>> list = ElasticClient.queryList(new Query(query_geo).prefix("content", "Android"));
        //Assert.assertEquals( 1 , list.size() );

    }


    @Test
    public void test_query_String() {


        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).queryString("Android"));

        Assert.assertEquals(2, list.size());

        list = elasticClient.queryList(new Query(query_test).queryString("content:Android AND date:2015-10-01"));

        Assert.assertEquals(1, list.size());

    }

    @Test
    public void test_and() {
        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).where(Range.greater("score", 0.3), Range.less("score", 0.8)));
        Assert.assertEquals(4, list.size());
    }


    @Test
    public void test_or() {
        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).or(Range.less("score", 0.4), Range.greater("score", 0.8)));
        Assert.assertEquals(5, list.size());
    }

    @Test
    public void test_and_or() {
        List<Map<String, Object>> list = elasticClient.queryList(new Query(query_test).where(
                And.on(FieldEqual.on("date", "2015-10-20")),
                Or.on(FieldEqual.on("star", 91).asFilter(), FieldEqual.on("star", 72).asFilter())
        ));
        Assert.assertEquals(2, list.size());

    }

}
