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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.immutables.value.Value;

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.isBinaryComparison;

/**
 * Rule that pushes filter into the spool.
 */
@Value.Enclosing
public class CorrelatedNestedLoopHashIndexSpoolRule extends RelRule<CorrelatedNestedLoopHashIndexSpoolRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    private CorrelatedNestedLoopHashIndexSpoolRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final IgniteCorrelatedNestedLoopJoin join = call.rel(0);
        final IgniteTableSpool spool = call.rel(2);

        RelOptCluster cluster = spool.getCluster();
        RexBuilder builder = RexUtils.builder(cluster);

        RelTraitSet trait = spool.getTraitSet();

        RelNode input = spool.getInput();

        RexNode condition0 = RexUtil.expandSearch(builder, null, spool.condition());

        condition0 = RexUtil.toCnf(builder, condition0);

        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition0);

        //TODO: https://issues.apache.org/jira/browse/IGNITE-14916
        for (RexNode rexNode : conjunctions)
            if (!isBinaryComparison(rexNode))
                return;

        List<RexNode> searchRow = RexUtils.buildHashSearchRow(
            cluster,
            condition0,
            spool.getRowType(),
            null,
            false
        );

        if (F.isEmpty(searchRow))
            return;

        RelNode hashSpool = new IgniteHashIndexSpool(
            cluster,
            trait.replace(RelCollations.EMPTY),
            input,
            searchRow,
            spool.condition()
        );

        RelNode res = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), hashSpool, join.getJoinType(),
            join.isSemiJoinDone());

        call.transformTo(res);
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableCorrelatedNestedLoopHashIndexSpoolRule.Config.of()
            .withDescription("FilterSpoolMergeToHashIndexSpoolRule")
            .withOperandFor(IgniteCorrelatedNestedLoopJoin.class, IgniteTableSpool.class);

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Join> joinCls, Class<? extends Spool> spoolCls) {
            return withOperandSupplier(
                o0 -> o0.operand(joinCls)
                    .inputs(
                        o1 -> o1.operand(RelNode.class).anyInputs(),
                        o2 -> o2.operand(spoolCls).anyInputs()))
                .as(Config.class);
        }

        /** {@inheritDoc} */
        @Override default CorrelatedNestedLoopHashIndexSpoolRule toRule() {
            return new CorrelatedNestedLoopHashIndexSpoolRule(this);
        }
    }
}
