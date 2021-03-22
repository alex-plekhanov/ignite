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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableRangeSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Sarg;

/**
 * Converts scalar IN to UNION ALL.
 */
public class ScalarInToUnionRule extends RelRule<ScalarInToUnionRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private ScalarInToUnionRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();

        RexNode dnf = RexUtil.toDnf(cluster.getRexBuilder(), rel.getCondition());

        if (!dnf.isA(SqlKind.SEARCH))
            return;

        List<RexNode> operands = ((RexCall)dnf).getOperands();

        assert operands.size() == 2 : "Unexpected operands count: " + operands.size();

        final RexNode ref = operands.get(0);
        final RexLiteral literal = (RexLiteral)operands.get(1);

        RelNode input = rel.getInput(0);

        //System.out.println(RelOptUtil.toString(createUnionAll(cluster, input, ref, literal)));

        call.transformTo(rel, Collections.singletonMap(
            createUnionAll(cluster, input, ref, literal), rel
        ));
    }

    /**
     * Creates 'UnionAll' for SEARCH/Sarg.
     *
     * @param cluster The cluster UnionAll expression will belongs to.
     * @param input Input.
     * @param ref Ref.
     * @param literal Search arguments.
     * @return UnionAll expression.
     */
    private RelNode createUnionAll(RelOptCluster cluster, RelNode input, RexNode ref, RexLiteral literal) {
        Sarg<?> sarg = literal.getValueAs(Sarg.class);

        RelBuilder relBldr = relBuilderFactory.create(cluster, null);
        RexBuilder rexBldr = relBldr.getRexBuilder();

        int[] cnt = new int[1];

        sarg.rangeSet.asRanges().forEach(range -> {
            // TODO process containsNull correctly
            Sarg<?> sarg2 = Sarg.of(sarg.containsNull, ImmutableRangeSet.of(range));

            relBldr.push(input).filter(relBldr.call(SqlStdOperatorTable.SEARCH, ref,
                rexBldr.makeSearchArgumentLiteral(sarg2, literal.getType())));

            cnt[0]++;
        });

        return relBldr.union(true, cnt[0]).build();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withDescription("ScalarInToUnionRule")
            .withOperandSupplier(o -> o.operand(LogicalFilter.class).anyInputs())
            .as(Config.class);

        /** {@inheritDoc} */
        @Override default ScalarInToUnionRule toRule() {
            return new ScalarInToUnionRule(this);
        }
    }
}
