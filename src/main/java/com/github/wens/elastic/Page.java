package com.github.wens.elastic;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by wens on 15-10-22.
 */
public class Page {


    public static final int DEFAULT_PAGE_SIZE = 25;
    private int start;
    private List<Map<String, Object>> items = Collections.emptyList();
    private boolean more;
    private int pageSize = DEFAULT_PAGE_SIZE;

    public Page start(int start) {
        this.start = start;
        return this;
    }

    public Page items(List<Map<String, Object>> items) {
        this.items = items;
        return this;
    }

    public Page more(boolean more) {
        this.more = more;
        return this;
    }

    public Page pageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }


    public int getStart() {
        return start;
    }

    public List<Map<String, Object>> getItems() {
        return items;
    }

    public boolean isMore() {
        return more;
    }

    public int getPageSize() {
        return pageSize;
    }
}
