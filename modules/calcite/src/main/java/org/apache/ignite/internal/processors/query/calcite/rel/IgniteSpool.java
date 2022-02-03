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

import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 * Base class for Ignite spool relational nodes.
 */
public abstract class IgniteSpool extends Spool implements TraitsAwareIgniteRel {
    /** Filters. */
    protected final RexNode condition;

    /** */
    public RexNode condition() {
        return condition;
    }

    /** */
    protected IgniteSpool(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, Type readType, Type writeType, RexNode condition) {
        super(cluster, traitSet, input, readType, writeType);
        this.condition = condition;
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * mq.getSelectivity(this, null);
    }

    /**
     * Compute rewind cost of spool.
     */
    public RelOptCost computeRewindCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost = computeSelfCost(planner, mq);

        // Self cost without memory and network.
        return planner.getCostFactory().makeCost(cost.getRows(), cost.getCpu(), cost.getIo());
    };

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughRewindability(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE), inTraits);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE), inTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.distribution(inTraits.get(0))), inTraits));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {

        Set<CorrelationId> corrSet = RexUtils.extractCorrelationIds(condition());

        CorrelationTrait correlation = TraitUtils.correlation(nodeTraits);

        if (corrSet.isEmpty() || correlation.correlationIds().containsAll(corrSet))
            return Pair.of(nodeTraits, ImmutableList.of(inTraits.get(0).replace(CorrelationTrait.UNCORRELATED)));
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(condition);

        if (TraitUtils.correlation(inTraits.get(0)).correlated())
            return null;
        else
            return ImmutableList.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }
}
