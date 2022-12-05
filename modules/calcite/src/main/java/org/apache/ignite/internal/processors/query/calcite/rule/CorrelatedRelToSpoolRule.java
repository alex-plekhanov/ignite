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

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.RuleFactoryConfig;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.immutables.value.Value;

/**
 * Rule that converts correlated project or filter to spool with project or filter.
 */
@Value.Enclosing
public class CorrelatedRelToSpoolRule extends RelRule<CorrelatedRelToSpoolRule.Config> {
    /** Instance. */
    public static final RelOptRule PROJECT = Config.PROJECT.toRule();

    /** Instance. */
    public static final RelOptRule FILTER = Config.FILTER.toRule();

    /** */
    private CorrelatedRelToSpoolRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final IgniteRel rel = call.rel(0);
        IgniteProject project = call.rule == PROJECT ? (IgniteProject)rel : null;
        IgniteFilter filter = call.rule == FILTER ? (IgniteFilter)rel : null;

        RelOptCluster cluster = rel.getCluster();

        RelTraitSet traits = rel.getTraitSet()
            .replace(RewindabilityTrait.REWINDABLE);

        IgniteTableSpool spool = new IgniteTableSpool(
            cluster,
            traits,
            Spool.Type.LAZY,
            filter != null ? filter.getCondition() : null,
            project != null ? project.getProjects() : null,
            convert(rel.getInput(0), rel.getInput(0).getTraitSet()
                .replace(RewindabilityTrait.ONE_WAY)
                .replace(CorrelationTrait.UNCORRELATED)
            )
        );

        call.transformTo(spool);
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        /** */
        Config DEFAULT = ImmutableCorrelatedRelToSpoolRule.Config.builder()
            .withRuleFactory(CorrelatedRelToSpoolRule::new)
            .build();

        /** */
        Config FILTER = DEFAULT.withRuleConfig(
            IgniteFilter.class, "CorrelatedFilterToSpoolRule", f -> RexUtils.hasCorrelation(f.getCondition()));

        /** */
        Config PROJECT = DEFAULT.withRuleConfig(
            IgniteProject.class, "CorrelatedProjectToSpoolRule", p -> RexUtils.hasCorrelation(p.getProjects()));

        /** */
        default <T extends RelNode> Config withRuleConfig(Class<T> relCls, String desc, Predicate<T> predicate) {
            return withDescription(desc)
                .withOperandSupplier(b0 -> b0.operand(relCls)
                    .predicate(predicate)
                    .anyInputs())
                .as(Config.class);
        }
    }
}
