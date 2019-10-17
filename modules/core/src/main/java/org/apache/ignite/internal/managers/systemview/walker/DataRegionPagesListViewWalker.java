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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesListView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * {@link PagesListView} attributes walker for data regions.
 * 
 * @see PagesListView
 */
public class DataRegionPagesListViewWalker implements SystemViewRowAttributeWalker<PagesListView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "name", String.class);
        v.accept(1, "bucketNumber", int.class);
        v.accept(2, "bucketSize", long.class);
        v.accept(3, "stripesCount", int.class);
        v.accept(4, "cachedPagesCount", int.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(PagesListView row, AttributeWithValueVisitor v) {
        v.accept(0, "name", String.class, row.name());
        v.acceptInt(1, "bucketNumber", row.bucketNumber());
        v.acceptLong(2, "bucketSize", row.bucketSize());
        v.acceptInt(3, "stripesCount", row.stripesCount());
        v.acceptInt(4, "cachedPagesCount", row.cachedPagesCount());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 5;
    }
}
