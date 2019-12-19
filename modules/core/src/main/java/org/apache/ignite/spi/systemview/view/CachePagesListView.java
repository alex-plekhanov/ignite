/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.ViewAttribute;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;

/**
 * Pages-list representation for a {@link SystemView}.
 */
public class CachePagesListView extends PagesListView {
    /** Partition id. */
    private final int partId;

    /**
     * @param pagesList Pages list.
     * @param bucket Bucket number.
     */
    public CachePagesListView(PagesList pagesList, int bucket, int partId) {
        super(pagesList, bucket);

        this.partId = partId;
    }

    /**
     * @return Cache group id.
     */
    @ViewAttribute(filtering = true)
    public int cacheGroupId() {
        return pagesList.groupId();
    }

    /**
     * @return Partition id.
     */
    @ViewAttribute(order = 1, filtering = true)
    public int partId() {
        return partId;
    }
}
