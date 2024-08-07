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

import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.systemview.view.sql.SqlViewView;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link SqlViewView} attributes walker.
 * 
 * @see SqlViewView
 */
public class SqlViewViewWalker implements SystemViewRowAttributeWalker<SqlViewView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "schema", String.class);
        v.accept(1, "name", String.class);
        v.accept(2, "sql", String.class);
        v.accept(3, "description", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(SqlViewView row, AttributeWithValueVisitor v) {
        v.accept(0, "schema", String.class, row.schema());
        v.accept(1, "name", String.class, row.name());
        v.accept(2, "sql", String.class, row.sql());
        v.accept(3, "description", String.class, row.description());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 4;
    }
}
