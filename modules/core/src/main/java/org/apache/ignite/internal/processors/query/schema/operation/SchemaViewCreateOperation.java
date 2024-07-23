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

package org.apache.ignite.internal.processors.query.schema.operation;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema index create operation.
 */
public class SchemaViewCreateOperation extends SchemaAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** View name. */
    private final String viewName;

    /** View SQL. */
    private final String viewSql;

    /** Replace view if exists. */
    private final boolean replace;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param viewName View name.
     * @param viewSql View SQL.
     * @param replace Replace view if exists.
     */
    public SchemaViewCreateOperation(
        UUID opId,
        String cacheName,
        String schemaName,
        String viewName,
        String viewSql,
        boolean replace
    ) {
        super(opId, cacheName, schemaName);

        this.viewName = viewName;
        this.viewSql = viewSql;
        this.replace = replace;
    }

    /**
     * @return View name.
     */
    public String viewName() {
        return viewName;
    }

    /**
     * @return View SQL.
     */
    public String viewSql() {
        return viewSql;
    }

    /**
     * @return Replace view if exists.
     */
    public boolean replace() {
        return replace;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaViewCreateOperation.class, this, "parent", super.toString());
    }
}
