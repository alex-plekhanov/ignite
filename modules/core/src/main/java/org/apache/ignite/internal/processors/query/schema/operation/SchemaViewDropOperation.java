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
 * Schema view drop operation.
 */
public class SchemaViewDropOperation extends SchemaAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** View name. */
    private final String viewName;

    /** Ignore operation if view doesn't exist. */
    private final boolean ifExists;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param viewName View name.
     * @param ifExists Ignore operation if view doesn't exist.
     */
    public SchemaViewDropOperation(UUID opId, String cacheName, String schemaName, String viewName, boolean ifExists) {
        super(opId, cacheName, schemaName);

        this.viewName = viewName;
        this.ifExists = ifExists;
    }

    /**
     * @return View name.
     */
    public String viewName() {
        return viewName;
    }

    /**
     * @return Ignore operation if view doesn't exist.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaViewDropOperation.class, this, "parent", super.toString());
    }
}
