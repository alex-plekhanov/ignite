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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.distribution;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public abstract class IgniteMdBaseCumulativeCost {
    /** */
    public abstract RelOptCost recursiveCost(RelNode rel, RelMetadataQuery mq);

    /** */
    public RelOptCost computeCost(RelSubset rel, RelMetadataQuery mq) {
        return VolcanoUtils.bestCost(rel);
    }

    /** */
    public RelOptCost computeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = nonCumulativeCost(rel, mq);

        if (cost.isInfinite())
            return cost;

        for (RelNode input : rel.getInputs()) {
            RelOptCost inputCost = recursiveCost(input, mq);
            if (inputCost.isInfinite())
                return inputCost;

            cost = cost.plus(inputCost);
        }

        return cost;
    }

    /** */
    public RelOptCost computeCost(IgniteCorrelatedNestedLoopJoin rel, RelMetadataQuery mq) {
        RelOptCost cost = nonCumulativeCost(rel, mq);

        if (cost.isInfinite())
            return cost;

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        Set<CorrelationId> corIds = rel.getVariablesSet();

        RelOptCost leftCost = mq.getCumulativeCost(left);
        if (leftCost.isInfinite())
            return leftCost;

        RelOptCost rightCost = mq.getCumulativeCost(right);
        if (rightCost.isInfinite())
            return rightCost;

        double rewindsCnt = left.estimateRowCount(mq) / corIds.size();

        RelOptCost rightRewindCost = cumulativeRewindCost(right, mq);
        if (rightRewindCost.isInfinite())
            return rightRewindCost;

        RelOptCost totalCost = cost.plus(leftCost).plus(rightCost);

        if (rewindsCnt > 1.0d)
            totalCost = totalCost.plus(rightRewindCost.multiplyBy(rewindsCnt - 1));

        return totalCost;
    }

    /** */
    protected static RelOptCost nonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = mq.getNonCumulativeCost(rel);

        if (cost.isInfinite())
            return cost;

        RelOptCostFactory costFactory = rel.getCluster().getPlanner().getCostFactory();

        if (rel.getConvention() == Convention.NONE || distribution(rel) == any())
            return costFactory.makeInfiniteCost();

        return costFactory.makeZeroCost().isLt(cost) ? cost : costFactory.makeTinyCost();
    }

    /**
     * Cumulative rewind cost calculation entry point.
     * @param rel Root node of a calculated fragment.
     * @param mq Metadata query instance.
     * @return Cumulative rewind cost.
     */
    protected static RelOptCost cumulativeRewindCost(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return ((RelMetadataQueryEx)mq).getCumulativeRewindCost(rel);
    }
}
