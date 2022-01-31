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
package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.immutables.value.Value;

/**
 * Rule that pushes filter into the spool.
 */
@Value.Enclosing
public class CorrelatedNestedLoopTableSpoolRule extends RelRule<CorrelatedNestedLoopTableSpoolRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    private CorrelatedNestedLoopTableSpoolRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final IgniteCorrelatedNestedLoopJoin join = call.rel(0);
        final IgniteFilter filter = call.rel(2);

        RelOptCluster cluster = join.getCluster();

        RelTraitSet trait = filter.getTraitSet().replace(RewindabilityTrait.REWINDABLE);

        RelNode input = filter.getInput();

/*
        if (TraitUtils.rewindability(input).rewindable())
            return;

*/

        RelNode spool = new IgniteTableSpool(
            cluster,
            trait,
            Spool.Type.LAZY,
            convert(input, input.getTraitSet()
                .replace(RewindabilityTrait.ONE_WAY)
                .replace(CorrelationTrait.UNCORRELATED)),
            filter.getCondition()
        );

        RelNode res = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), spool, join.getJoinType(),
            join.isSemiJoinDone());

        call.transformTo(res);
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableCorrelatedNestedLoopTableSpoolRule.Config.of()
            .withDescription("CorrelatedNestedLoopTableSpoolRule")
            .withOperandSupplier(
                o0 -> o0.operand(IgniteCorrelatedNestedLoopJoin.class)
                .inputs(
                    o1 -> o1.operand(RelNode.class).anyInputs(),
                    o2 -> o2.operand(IgniteFilter.class).anyInputs()))
            .as(Config.class);

        /** {@inheritDoc} */
        @Override default CorrelatedNestedLoopTableSpoolRule toRule() {
            return new CorrelatedNestedLoopTableSpoolRule(this);
        }
    }
}
