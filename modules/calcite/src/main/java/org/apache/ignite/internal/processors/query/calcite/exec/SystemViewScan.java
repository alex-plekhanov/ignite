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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.SystemViewDescriptorImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/** */
public class SystemViewScan<Row, ViewRow> implements Iterable<Row> {
    /** */
    private final Predicate<Row> filters;

    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    private final SystemViewDescriptorImpl<ViewRow> desc;

    /** */
    private final RowFactory<Row> factory;

    /** */
    private final Function<Row, Row> rowTransformer;

    /** Participating colunms. */
    private final ImmutableBitSet requiredColunms;

    /** */
    public SystemViewScan(
        ExecutionContext<Row> ectx,
        SystemViewDescriptorImpl<ViewRow> desc,
        Predicate<Row> filters,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColunms
    ) {
        this.ectx = ectx;
        this.desc = desc;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColunms = requiredColunms;

        RelDataType rowType = desc.rowType(this.ectx.getTypeFactory(), requiredColunms);

        factory = this.ectx.rowHandler().factory(this.ectx.getTypeFactory(), rowType);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> iterator() {
        Iterator<Row> iter = F.iterator(
            desc.systemView().iterator(),
            row -> desc.toRow(ectx, row, factory, requiredColunms),
            true);

        if (rowTransformer != null || filters != null) {
            IgniteClosure<Row, Row> trans = rowTransformer == null ? F.identity() : rowTransformer::apply;
            IgnitePredicate<Row> filter = filters == null ? F.alwaysTrue() : filters::test;

            iter = F.iterator(iter, trans, true, filter);
        }

        return iter;
    }
}
