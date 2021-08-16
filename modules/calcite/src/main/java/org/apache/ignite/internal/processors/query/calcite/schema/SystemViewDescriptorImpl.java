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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;

/**
 *
 */
public class SystemViewDescriptorImpl<ViewRow> extends NullInitializerExpressionFactory
    implements TableDescriptor<ViewRow> {
    /** */
    private static final ColumnDescriptor<?>[] DUMMY = new ColumnDescriptor[0];

    /** */
    private final ColumnDescriptor<ViewRow>[] descriptors;

    /** */
    private final Map<String, ColumnDescriptor<ViewRow>> descriptorsMap;

    /** */
    private final SystemView<ViewRow> sysView;

    /** */
    public SystemViewDescriptorImpl(SystemView<ViewRow> sysView) {
        List<ColumnDescriptor<ViewRow>> descriptors = new ArrayList<>(sysView.walker().count());

        sysView.walker().visitAll(new SystemViewRowAttributeWalker.AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                descriptors.add(new ViewColumnDescriptor<ViewRow>(toSqlName(name), clazz, idx));
            }
        });

        Map<String, ColumnDescriptor<ViewRow>> descriptorsMap = U.newHashMap(descriptors.size());

        for (ColumnDescriptor<ViewRow> descriptor : descriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        this.sysView = sysView;
        this.descriptors = descriptors.toArray((ColumnDescriptor<ViewRow>[])DUMMY);
        this.descriptorsMap = descriptorsMap;
    }

    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, null);
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<?, ?> cacheContext() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContextInfo<?, ?> cacheInfo() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return IgniteDistributions.single();
    }

    /** {@inheritDoc} */
    @Override public boolean match(ViewRow row) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public <Row> Row toRow(
        ExecutionContext<Row> ectx,
        ViewRow row,
        RowHandler.RowFactory<Row> factory,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        RowHandler<Row> hnd = factory.handler();

        assert hnd == ectx.rowHandler();

        Row res = factory.create();

        assert hnd.columnCount(res) == (requiredColumns == null ? descriptors.length : requiredColumns.cardinality());

        sysView.walker().visitAll(row, new SystemViewRowAttributeWalker.AttributeWithValueVisitor() {
            private int colIdx = 0;

            @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
                if (requiredColumns == null || requiredColumns.get(idx))
                    hnd.set(colIdx++, res, TypeUtils.toInternal(ectx, val, descriptors[idx].storageType()));
            }

            @Override public void acceptBoolean(int idx, String name, boolean val) {
                accept(idx, name, Boolean.class, val);
            }

            @Override public void acceptChar(int idx, String name, char val) {
                accept(idx, name, Character.class, val);
            }

            @Override public void acceptByte(int idx, String name, byte val) {
                accept(idx, name, Byte.class, val);
            }

            @Override public void acceptShort(int idx, String name, short val) {
                accept(idx, name, Short.class, val);
            }

            @Override public void acceptInt(int idx, String name, int val) {
                accept(idx, name, Integer.class, val);
            }

            @Override public void acceptLong(int idx, String name, long val) {
                accept(idx, name, Long.class, val);
            }

            @Override public void acceptFloat(int idx, String name, float val) {
                accept(idx, name, Float.class, val);
            }

            @Override public void acceptDouble(int idx, String name, double val) {
                accept(idx, name, Double.class, val);
            }
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <Row> ModifyTuple toTuple(
        ExecutionContext<Row> ectx,
        Row row,
        TableModify.Operation op,
        Object arg
    ) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++)
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }
        else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1))
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }

        return TypeUtils.sqlType(factory, b.build());
    }

    /** {@inheritDoc} */
    @Override public ColumnDescriptor<ViewRow> columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return ColocationGroup.forNodes(Collections.singletonList(ctx.localNodeId()));
    }

    /** */
    public SystemView<ViewRow> systemView() {
        return sysView;
    }

    /** */
    private static class ViewColumnDescriptor<ViewRow> implements ColumnDescriptor<ViewRow> {
        /** */
        private final String name;

        /** */
        private final int fieldIdx;

        /** */
        private final Class<?> type;

        /** */
        private ViewColumnDescriptor(String name, Class<?> type, int fieldIdx) {
            this.name = name;
            this.fieldIdx = fieldIdx;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public boolean field() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public int fieldIndex() {
            return fieldIdx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            return f.toSql(f.createJavaType(type));
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, ViewRow src) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) {
            throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescription() {
        return null;
    }
}
