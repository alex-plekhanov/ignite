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

package org.apache.ignite.internal.processors.query.calcite.rel.set;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * Base class for physical MINUS (EXCEPT) set op.
 */
public abstract class IgniteMinus extends Minus implements IgniteSetOp {
    /** Count of counter fields used to aggregate results. */
    protected static final int COUNTER_FIELDS_CNT = 2;

    /** */
    IgniteMinus(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
    }

    /** {@inheritDoc} */
    protected IgniteMinus(RelInput input) {
        super(TraitUtils.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        final List<RelNode> inputs = getInputs();

        double rows = mq.getRowCount(inputs.get(0));

        for (int i = 1; i < inputs.size(); i++)
            rows -= 0.5 * Math.min(rows, mq.getRowCount(inputs.get(i)));

        return rows;
    }

    /** {@inheritDoc} */
    @Override public boolean all() {
        return all;
    }
}
