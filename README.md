# easy-elastic4j

elasticsearch原生client api相对来说还是比较复杂的,大多数的api在实际的业务场景中可能用不到,easy-elastic4j只是封装了常见的操作,简化了使用难度,对初学者也非常友好.

## Example

```

        //create elastic client
        ElasticClient elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);

        //index 
        IndexObject indexObject = new IndexObject("1");

        indexObject.field("name", "wens");
        indexObject.field("age", 29);
        indexObject.field("height", 165.5f);
        indexObject.field("comment", "攻城狮");
        elasticClient.index(test_index, indexObject);
        
        //bluk index
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

        elasticClient.index(test_index, list);
        
        //query for page
        Page page = elasticClient.queryPage(new Query(test_index).eq("group", "coder").limit(0, 5).orderByAsc("name"));
        Assert.assertEquals(5, page.getItems().size());
        Assert.assertEquals(true, page.isMore());
        int id = 0;
        for (Map<String, Object> item : page.getItems()) {
            Assert.assertEquals(String.format("wens%05d", id++), item.get("name"));
        }
        
        //query geo
        double lat = 23.131187;
        double lon = 113.257174;
        Query query = new Query(query_geo);
        query.nearby("location", lat, lon, 1000000);
        List<Map<String, Object>> list = elasticClient.queryList(query);
        double last = 0;
        for (Map<String, Object> item : list) {
            String location = (String) item.get("location");
            String[] point = location.split(",");
            double distance = GeoUtils.getDistance(lat, lon, Double.parseDouble(point[0]), Double.parseDouble(point[1]));
            Assert.assertEquals(true, last <= distance);
            System.out.println(item.get("name") + " " + location + " " + item.get("num") + " " + distance);
        }
        
        ......
        


```

# Query Condition

* FieldEqual 相等
* FieldNotEqual 不相等
* FieldExist 字段存在
* Nearby 地理位置附近
* NearbyRange 地理位置范围
* NoneInField not in匹配
* OneInField in匹配
* Prefix 前缀匹配
* QueryString 原生查询字符串
* Range 范围比较
* And and组合
* Or or组合
* Not 取反
