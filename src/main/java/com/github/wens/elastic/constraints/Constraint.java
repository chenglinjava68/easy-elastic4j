/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package com.github.wens.elastic.constraints;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by wens on 15-10-26.
 */
public interface Constraint {


    QueryBuilder createQuery();

    FilterBuilder createFilter();
}
