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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Local database schema object.
 */
public class GridQuerySchema {
    /** */
    private final String schemaName;

    /** */
    private final ConcurrentMap<String, GridQueryTable> tbls = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<QueryTypeNameKey, GridQueryTable> typeToTbl = new ConcurrentHashMap<>();

    /** Whether schema is predefined and cannot be dorpped. */
    private final boolean predefined;

    /** Usage count. */
    private int usageCnt;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param predefined Predefined flag.
     */
    public GridQuerySchema(String schemaName, boolean predefined) {
        this.schemaName = schemaName;
        this.predefined = predefined;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Increments counter for number of caches having this schema.
     */
    public void incrementUsageCount() {
        if (!predefined)
            ++usageCnt;
    }

    /**
     * Increments counter for number of caches having this schema.
     *
     * @return If schema is no longer used.
     */
    public boolean decrementUsageCount() {
        return !predefined && --usageCnt == 0;
    }

    /**
     * @return Tables.
     */
    public Collection<GridQueryTable> tables() {
        return tbls.values();
    }

    /**
     * @param tblName Table name.
     * @return Table.
     */
    public GridQueryTable tableByName(String tblName) {
        return tbls.get(tblName);
    }

    /**
     * @param typeName Type name.
     * @return Table.
     */
    public GridQueryTable tableByTypeName(String cacheName, String typeName) {
        return typeToTbl.get(new QueryTypeNameKey(cacheName, typeName));
    }

    /**
     * @param tbl Table descriptor.
     */
    public void add(GridQueryTable tbl) {
        if (tbls.putIfAbsent(tbl.descriptor().tableName(), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.descriptor().tableName());

        if (typeToTbl.putIfAbsent(new QueryTypeNameKey(tbl.cacheInfo().name(), tbl.descriptor().name()), tbl) != null)
            throw new IllegalStateException("Table already registered: " + tbl.descriptor().tableName());
    }

    /**
     * Drop table.
     *
     * @param tbl Table to be removed.
     */
    public void drop(GridQueryTable tbl) {
        tbls.remove(tbl.descriptor().tableName());

        typeToTbl.remove(new QueryTypeNameKey(tbl.cacheInfo().name(), tbl.descriptor().name()));
    }

    /**
     * @return {@code True} if schema is predefined.
     */
    public boolean predefined() {
        return predefined;
    }
}
