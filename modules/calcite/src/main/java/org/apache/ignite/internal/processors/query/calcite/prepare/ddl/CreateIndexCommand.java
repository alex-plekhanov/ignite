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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.List;

/**
 * CREATE INDEX statement.
 */
public class CreateIndexCommand implements DdlCommand {
    /**
     * Schema name upon which this statement has been issued - <b>not</b> the name of the schema where this new index
     * will be created.
     */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Table name. */
    private String idxName;

    /** Name of new cache associated with this table. */
    private String cacheName;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Indexed columns. */
    private List<String> cols;

    /** Columns order. */
    private List<Boolean> asc;

    /**
     * @return Name of new cache associated with this table.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Name of new cache associated with this table.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Columns.
     */
    public List<String> columns() {
        return cols;
    }

    /**
     * @param cols Columns.
     */
    public void columns(List<String> cols) {
        this.cols = cols;
    }

    /**
     * @return Columns order.
     */
    public List<Boolean> asc() {
        return asc;
    }

    /**
     * @param asc Columns order.
     */
    public void asc(List<Boolean> asc) {
        this.asc = asc;
    }

    /**
     * @return Schema name upon which this statement has been issued.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name upon which this statement has been issued.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @param idxName Index name.
     */
    public void indexName(String idxName) {
        this.idxName = idxName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return Quietly ignore this command if index already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly ignore this command if index already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
