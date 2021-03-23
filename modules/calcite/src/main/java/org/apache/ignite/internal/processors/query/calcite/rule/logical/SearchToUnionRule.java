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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
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
 * Converts SEARCH expression to UNION ALL.
 */
public class SearchToUnionRule extends RelRule<SearchToUnionRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private SearchToUnionRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);

        RexNode cnf = RexUtil.toCnf(rel.getCluster().getRexBuilder(), rel.getCondition());

        if (cnf.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall)cnf).getOperands();

            Map<RelNode, RelNode> equivs = null;

            for (RexNode node : operands) {
                if (node.isA(SqlKind.SEARCH)) {
                    RelNode equiv = createUnionAll(rel, operands, node);

                    if (equiv != null) {
                        if (equivs == null)
                            equivs = new HashMap<>();

                        equivs.put(equiv, rel);
                    }
                }
            }

            if (equivs != null)
                call.transformTo(rel, equivs);
        }
        else if (cnf.isA(SqlKind.SEARCH)) {
            RelNode equiv = createUnionAll(rel, null, cnf);

            if (equiv != null)
                call.transformTo(rel, Collections.singletonMap(equiv, rel));
        }
    }

    /**
     * Split SEARCH to UnionAll with single range SEARCH.
     *
     * @return UnionAll expression or null if can't split this SEARCH filter.
     */
    private RelNode createUnionAll(LogicalFilter rel, List<RexNode> predicates, RexNode search) {
        RelBuilder relBldr = relBuilderFactory.create(rel.getCluster(), null);
        RexBuilder rexBldr = relBldr.getRexBuilder();

        List<RexNode> operands = ((RexCall)search).getOperands();

        assert operands.size() == 2 && operands.get(1) instanceof RexLiteral : "Unexpected operands: " + operands;

        final RexNode ref = operands.get(0);
        final RexLiteral literal = (RexLiteral)operands.get(1);

        Sarg<?> sarg = literal.getValueAs(Sarg.class);

        // Skip split for 'IS NULL'.
        if (sarg.containsNull)
            return null;

        Set<? extends Range<?>> ranges = sarg.rangeSet.asRanges();

        if (ranges.size() <= 1)
            return null; // Nothing to split.

        RelNode input = rel.getInput(0);

        int[] cnt = new int[1];

        RexNode[] extraPred;
        int searchPredIdx;

        if (predicates != null) {
            extraPred = new RexNode[predicates.size()];
            predicates.toArray(extraPred);
            searchPredIdx = predicates.indexOf(search);
        }
        else {
            extraPred = null;
            searchPredIdx = -1;
        }

        ranges.forEach(range -> {
            cnt[0]++;
            relBldr.push(input);

            Sarg<?> sarg2 = Sarg.of(false, ImmutableRangeSet.of(range));

            RexNode searchFilter = relBldr.call(SqlStdOperatorTable.SEARCH, ref,
                rexBldr.makeSearchArgumentLiteral(sarg2, literal.getType()));

            if (extraPred != null) {
                RexNode[] filterPredicates = extraPred.clone();
                filterPredicates[searchPredIdx] = searchFilter;
                relBldr.filter(filterPredicates);
            }
            else
                relBldr.filter(searchFilter);

        });

        RelNode union = relBldr.union(true, cnt[0]).build();

        // TODO remove
        System.out.println(RelOptUtil.toString(union));

        return union;
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withDescription("SearchToUnionRule")
            .withOperandSupplier(o -> o.operand(LogicalFilter.class).anyInputs())
            .as(Config.class);

        /** {@inheritDoc} */
        @Override default SearchToUnionRule toRule() {
            return new SearchToUnionRule(this);
        }
    }
}
