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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;

/**
 * Callback for a relational expression to dump itself as Binary Object.
 *
 * @see RelBinaryObjectReader
 */
public class RelBinaryObjectWriter implements RelWriter {
    /** */
    private final RelBinaryObject relBinaryObj;

    /** */
    private final List<Object> relList = new ArrayList<>();

    /** */
    private final Map<RelNode, Integer> relIdMap = new IdentityHashMap<>();

    /** */
    private Integer previousId;

    /** */
    private List<Pair<String, Object>> items = new ArrayList<>();

    /** */
    public static byte[] toBinary(CacheObjectBinaryProcessorImpl binary, RelNode rel) {
        RelBinaryObjectWriter writer = new RelBinaryObjectWriter(binary, rel.getCluster());
        rel.explain(writer);

        return binary.marshal(writer.relList);
    }

    /** */
    public RelBinaryObjectWriter(CacheObjectBinaryProcessorImpl binary, RelOptCluster cluster) {
        relBinaryObj = new RelBinaryObject(binary.binary(), cluster.getPlanner().getContext().unwrap(BaseQueryContext.class));
    }

    /** {@inheritDoc} */
    @Override public final void explain(RelNode rel, List<Pair<String, Object>> valList) {
        explain_(rel, valList);
    }

    /** {@inheritDoc} */
    @Override public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    /** {@inheritDoc} */
    @Override public RelWriter item(String term, Object val) {
        items.add(Pair.of(term, val));
        return this;
    }

    /** {@inheritDoc} */
    @Override public RelWriter done(RelNode node) {
        List<Pair<String, Object>> cur0 = items;
        items = new ArrayList<>();
        explain_(node, cur0);
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean nest() {
        return true;
    }

    /** */
    private void explain_(RelNode rel, List<Pair<String, Object>> values) {
        BinaryObjectBuilder builder = relBinaryObj.builder(rel);

        builder.setField("id", 0); // ensure that id is the first attribute

        for (Pair<String, Object> val : values) {
            if (val.right instanceof RelNode)
                continue;

            builder.setField(val.left, relBinaryObj.toBinary(val.right));
        }
        // omit 'inputs: ["3"]' if "3" is the preceding rel
        final List<Integer> list = explainInputs(rel.getInputs());
        if (list.size() != 1 || !list.get(0).equals(previousId))
            builder.setField("inputs", list);

        final Integer id = relIdMap.size();
        relIdMap.put(rel, id);
        builder.setField("id", id);

        relList.add(builder.build());
        previousId = id;
    }

    /** */
    private List<Integer> explainInputs(List<RelNode> inputs) {
        final List<Integer> list = relBinaryObj.list();
        for (RelNode input : inputs) {
            Integer id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = previousId;
            }
            list.add(id);
        }
        return list;
    }
}
