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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;

/**
 * Local database table object.
 */
public class GridQueryTable {
    /** */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** */
    private final GridQueryTypeDescriptor desc;

    /**
     * Ctor.
     *
     * @param cacheInfo Cache cacheInfo context.
     * @param desc Descriptor.
     */
    public GridQueryTable(GridCacheContextInfo<?, ?> cacheInfo, GridQueryTypeDescriptor desc) {
        this.cacheInfo = cacheInfo;
        this.desc = desc;
    }

    /** */
    public GridCacheContextInfo<?, ?> cacheInfo() {
        return cacheInfo;
    }

    /** */
    public GridQueryTypeDescriptor descriptor() {
        return desc;
    }
}
