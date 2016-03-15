package com.github.wens.elastic;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15-10-27.
 */
public class Benchmark {

    private static final String bench_test = "bench_test";

    public static void main(String[] args) {

        int n = 50;
        final ElasticClient elasticClient = ElasticClientFactory.create(R.CLUSTER_NAME, R.SERVER_ADDRESSES);
        final Counter counter = new Counter();
        for (int i = 0; i < n; i++) {
            final String idPrefix = String.valueOf(i);
            new Thread() {
                @Override
                public void run() {
                    test_single(elasticClient, idPrefix, counter);
                    //test_batch(idPrefix, counter);
                }

            }.start();
        }
    }


    public static void test_single(ElasticClient elasticClient, String idPrefix, Counter counter) {
        long id = 0;
        while (true) {
            IndexObject indexObject = new IndexObject(idPrefix + id);
            indexObject.field("name", "wens");
            elasticClient.index(bench_test, indexObject);
            counter.increment();
        }
    }


    public static void test_batch(ElasticClient elasticClient, String idPrefix, Counter counter) {
        long id = 0;
        int batchSize = 100;
        List<IndexObject> list = new ArrayList<>(batchSize);
        while (true) {
            IndexObject indexObject = new IndexObject(idPrefix + id);
            indexObject.field("name", "wens");
            list.add(indexObject);
            if (list.size() >= batchSize) {
                elasticClient.index(bench_test, list);
                list = new ArrayList<>(batchSize);
                counter.increment(batchSize);
            }

        }

    }

    static class Counter implements Runnable {

        static AtomicLong count = new AtomicLong(0);

        Counter() {
            new Thread(this).start();
        }

        public void increment(int delta) {
            count.addAndGet(delta);
        }

        public void increment() {
            count.incrementAndGet();
        }

        @Override
        public void run() {
            while (true) {

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    //
                }

                long nowCount = count.get();
                System.out.println((nowCount / 5) + "   ops");
                count.set(0);

            }
        }
    }


}
