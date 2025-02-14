/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.externalize;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RelBinaryObjectReader {
    /** */
    private final RelOptSchema relOptSchema;

    /** */
    private final RelBinaryObject relBinaryObj;

    /** */
    private final GridBinaryMarshaller marshaller;

    /** */
    private final Map<Integer, RelNode> relMap = new LinkedHashMap<>();

    /** */
    private RelNode lastRel;

    /** */
    public static <T extends RelNode> T fromBinary(CacheObjectBinaryProcessorImpl binary, BaseQueryContext ctx, byte[] payload) {
        RelBinaryObjectReader reader = new RelBinaryObjectReader(binary, ctx);

        return (T)reader.read(payload);
    }

    /** */
    public RelBinaryObjectReader(CacheObjectBinaryProcessorImpl binary, BaseQueryContext qctx) {
        relOptSchema = qctx.catalogReader();

        marshaller = binary.marshaller();
        relBinaryObj = new RelBinaryObject(null, qctx);
    }

    /** */
    public RelNode read(byte[] payload) {
        lastRel = null;

        List<BinaryObject> rels = marshaller.unmarshal(payload, null);

        readRels(rels);

        return lastRel;
    }

    /** */
    private void readRels(List<BinaryObject> binaryRels) {
        for (BinaryObject binaryRel : binaryRels)
            readRel(binaryRel);
    }

    /** */
    private void readRel(BinaryObject bo) {
        Integer id = bo.field("id");
        String type = bo.type().typeName();
        Function<RelInput, RelNode> factory = relBinaryObj.factory(type);
        RelNode rel = factory.apply(new RelInputImpl(bo));
        relMap.put(id, rel);
        lastRel = rel;
    }

    /** */
    private class RelInputImpl implements RelInputEx {
        /** */
        private final BinaryObject binaryObjRel;

        /** */
        private RelInputImpl(BinaryObject binaryObjRel) {
            this.binaryObjRel = binaryObjRel;
        }

        /** {@inheritDoc} */
        @Override public RelOptCluster getCluster() {
            return Commons.emptyCluster();
        }

        /** {@inheritDoc} */
        @Override public RelTraitSet getTraitSet() {
            return Commons.emptyCluster().traitSet();
        }

        /** {@inheritDoc} */
        @Override public RelOptTable getTable(String table) {
            List<String> list = getStringList(table);
            return relOptSchema.getTableForMember(list);
        }

        /** {@inheritDoc} */
        @Override public RelNode getInput() {
            List<RelNode> inputs = getInputs();
            assert inputs.size() == 1;
            return inputs.get(0);
        }

        /** {@inheritDoc} */
        @Override public List<RelNode> getInputs() {
            List<Integer> inputIds = getIntegerList("inputs");

            if (inputIds == null)
                return ImmutableList.of(lastRel);

            List<RelNode> inputs = new ArrayList<>();

            for (Integer inputId : inputIds)
                inputs.add(lookupInput(inputId));

            return inputs;
        }

        /** {@inheritDoc} */
        @Override public RexNode getExpression(String tag) {
            return relBinaryObj.toRex(this, binaryObjRel.field(tag));
        }

        /** {@inheritDoc} */
        @Override public ImmutableBitSet getBitSet(String tag) {
            return ImmutableBitSet.of(getIntegerList(tag));
        }

        /** {@inheritDoc} */
        @Override public List<ImmutableBitSet> getBitSetList(String tag) {
            List<List<Integer>> list = getIntegerListList(tag);
            if (list == null)
                return null;
            ImmutableList.Builder<ImmutableBitSet> builder =
                ImmutableList.builder();
            for (List<Integer> integers : list)
                builder.add(ImmutableBitSet.of(integers));
            return builder.build();
        }

        /** {@inheritDoc} */
        @Override public List<String> getStringList(String tag) {
            return binaryObjRel.field(tag);
        }

        /** {@inheritDoc} */
        @Override public List<Integer> getIntegerList(String tag) {
            return binaryObjRel.field(tag);
        }

        /** {@inheritDoc} */
        @Override public List<List<Integer>> getIntegerListList(String tag) {
            return binaryObjRel.field(tag);
        }

        /** {@inheritDoc} */
        @Override public List<AggregateCall> getAggregateCalls(String tag) {
            List<BinaryObject> aggs = binaryObjRel.field(tag);
            List<AggregateCall> inputs = new ArrayList<>();

            for (BinaryObject agg : aggs)
                inputs.add(toAggCall(agg));

            return inputs;
        }

        /** {@inheritDoc} */
        @Override public Object get(String tag) {
            return binaryObjRel.field(tag);
        }

        /** {@inheritDoc} */
        @Override public String getString(String tag) {
            return binaryObjRel.field(tag);
        }

        /** {@inheritDoc} */
        @Override public float getFloat(String tag) {
            return ((Number)binaryObjRel.field(tag)).floatValue();
        }

        /** {@inheritDoc} */
        @Override public BigDecimal getBigDecimal(String tag) {
            return SqlFunctions.toBigDecimal((Object)binaryObjRel.field(tag));
        }

        /** {@inheritDoc} */
        @Override public boolean getBoolean(String tag, boolean dflt) {
            Boolean b = binaryObjRel.field(tag);
            return b != null ? b : dflt;
        }

        /** {@inheritDoc} */
        @Override public <E extends Enum<E>> E getEnum(String tag, Class<E> enumCls) {
            Object name = get(tag);

            if (name instanceof String) {
                // Some types of nodes (Join for joinType enum, for example) serialize names in lower case.
                E res = Util.enumVal(enumCls, ((String)name).toUpperCase(Locale.ROOT));

                if (res != null)
                    return res;
            }

            return relBinaryObj.toEnum(name);
        }

        /** {@inheritDoc} */
        @Override public List<RexNode> getExpressionList(String tag) {
            List<BinaryObject> boNodes = binaryObjRel.field(tag);

            if (boNodes == null)
                return null;

            List<RexNode> nodes = new ArrayList<>(boNodes.size());

            for (BinaryObject boNode : boNodes)
                nodes.add(relBinaryObj.toRex(this, boNode));

            return nodes;
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(String tag) {
            Object o = binaryObjRel.field(tag);
            return relBinaryObj.toType(Commons.typeFactory(Commons.emptyCluster()), o);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
            List<RexNode> expressionList = getExpressionList(expressionsTag);
            List<String> names = (List<String>)get(fieldsTag);

            return Commons.typeFactory(Commons.emptyCluster()).createStructType(
                new AbstractList<Map.Entry<String, RelDataType>>() {
                    @Override public Map.Entry<String, RelDataType> get(int idx) {
                        return Pair.of(names.get(idx), expressionList.get(idx).getType());
                    }

                    @Override public int size() {
                        return names.size();
                    }
                });
        }

        /** {@inheritDoc} */
        @Override public RelCollation getCollation() {
            return relBinaryObj.toCollation((List)get("collation"));
        }

        /** {@inheritDoc} */
        @Override public RelCollation getCollation(String tag) {
            return relBinaryObj.toCollation((List)get(tag));
        }

        /** {@inheritDoc} */
        @Override public List<SearchBounds> getSearchBounds(String tag) {
            return relBinaryObj.toSearchBoundList(this, (List<BinaryObject>)get(tag));
        }

        /** {@inheritDoc} */
        @Override public RelDistribution getDistribution() {
            return relBinaryObj.toDistribution(binaryObjRel.field("distribution"));
        }

        /** {@inheritDoc} */
        @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
            List<List> tuples = (List)get(tag);

            ImmutableList.Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();

            for (List tuple : tuples)
                builder.add(getTuple(tuple));

            return builder.build();
        }

        /** */
        private RelNode lookupInput(Integer inputId) {
            RelNode node = relMap.get(inputId);

            if (node == null)
                throw new RuntimeException("Unknown id " + inputId + " for relational expression");

            return node;
        }

        /** */
        private ImmutableList<RexLiteral> getTuple(List<BinaryObject> tuple) {
            ImmutableList.Builder<RexLiteral> builder = ImmutableList.builder();

            for (BinaryObject val : tuple)
                builder.add((RexLiteral)relBinaryObj.toRex(this, val));

            return builder.build();
        }

        /** */
        private AggregateCall toAggCall(BinaryObject agg) {
            SqlAggFunction aggregation = (SqlAggFunction)relBinaryObj.toOp(agg.field("agg"));
            Boolean distinct = agg.field("distinct");
            List<Integer> operands = agg.field("operands");
            Integer filterOperand = agg.field("filter");
            RelDataType type = relBinaryObj.toType(Commons.typeFactory(), agg.field("type"));
            String name = agg.field("name");
            RelCollation collation = relBinaryObj.toCollation(agg.field("coll"));
            List<RexNode> rexList = Commons.transform((List<BinaryObject>)agg.field("rexList"),
                node -> relBinaryObj.toRex(this, node));

            return AggregateCall.create(aggregation, distinct, false, false, rexList, operands,
                filterOperand == null ? -1 : filterOperand, null, collation, type, name);
        }
    }
}
