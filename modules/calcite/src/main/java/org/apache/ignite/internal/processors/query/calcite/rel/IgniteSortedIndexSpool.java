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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;

/**
 * Relational operator that returns the sorted contents of a table
 * and allow to lookup rows by specified bounds.
 */
public class IgniteSortedIndexSpool extends IgniteSpool {
    /** */
    private final RelCollation collation;

    /** Index condition. */
    private final IndexConditions idxCond;

    /** */
    public IgniteSortedIndexSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RelCollation collation,
        RexNode condition,
        IndexConditions idxCond
    ) {
        super(cluster, traits, input, Type.LAZY, Type.EAGER, condition);

        assert Objects.nonNull(idxCond);
        assert Objects.nonNull(condition);

        this.idxCond = idxCond;
        this.collation = collation;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteSortedIndexSpool(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getCollation(),
            input.getExpression("condition"),
            new IndexConditions(input)
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSortedIndexSpool(cluster, getTraitSet(), inputs.get(0), collation, condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteSortedIndexSpool(getCluster(), traitSet, input, collation, condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        writer.item("condition", condition);
        writer.item("collation", collation);

        return idxCond.explainTerms(writer);
    }

    /** */
    public IndexConditions indexCondition() {
        return idxCond;
    }

    /** */
    @Override public RelCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost;

        double selectivity = mq.getSelectivity(this, condition);

        if (idxCond.lowerCondition() != null || idxCond.upperCondition() != null) {
            cpuCost = Math.log(rowCnt) * IgniteCost.ROW_COMPARISON_COST +
                rowCnt * selectivity * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST);
        }
        else
            cpuCost = rowCnt * IgniteCost.ROW_PASS_THROUGH_COST;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelCollation required = TraitUtils.collation(nodeTraits);

        if (required.satisfies(collation))
            return Pair.of(nodeTraits, ImmutableList.of(inputTraits.get(0).replace(required)));
        else if (collation.satisfies(required))
            return Pair.of(nodeTraits.replace(collation), ImmutableList.of(inputTraits.get(0).replace(collation)));
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelCollation inputCollation = TraitUtils.collation(inputTraits.get(0));

        if (inputCollation.satisfies(collation))
            return ImmutableList.of(Pair.of(nodeTraits.replace(inputCollation), inputTraits));
        else {
            return ImmutableList.of(Pair.of(nodeTraits.replace(collation),
                ImmutableList.of(inputTraits.get(0).replace(collation))));
        }
    }
}
