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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.RuleFactoryConfig;
import org.immutables.value.Value;

/**
 * Rule that pushes filter into the spool.
 */
@Value.Enclosing
public class SpoolRemoveRule extends RelRule<SpoolRemoveRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    private SpoolRemoveRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        call.transformTo(call.rel(1));
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        /** */
        Config DEFAULT = ImmutableSpoolRemoveRule.Config.builder()
            .withDescription("SpoolRemoveRule")
            .withRuleFactory(SpoolRemoveRule::new)
            .withOperandSupplier(b0 -> b0.operand(IgniteTableSpool.class)
                .inputs(b1 -> b1.operand(IgniteExchange.class).anyInputs()))
            .build();
    }
}
