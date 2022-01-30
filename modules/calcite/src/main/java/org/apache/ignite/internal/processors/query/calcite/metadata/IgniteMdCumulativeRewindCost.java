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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSpool;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdCumulativeRewindCost extends IgniteMdBaseCumulativeCost
    implements MetadataHandler<IgniteMetadata.CumulativeRewindCostMetadata> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        IgniteMethod.CUMULATIVE_REWIND_COST.method(), new IgniteMdCumulativeRewindCost());

    /** {@inheritDoc} */
    @Override public MetadataDef<IgniteMetadata.CumulativeRewindCostMetadata> getDef() {
        return IgniteMetadata.CumulativeRewindCostMetadata.DEF;
    }

    /** */
    public RelOptCost getCumulativeRewindCost(RelSubset rel, RelMetadataQuery mq) {
        return computeCost(rel, mq);
    }

    /** */
    public RelOptCost getCumulativeRewindCost(RelNode rel, RelMetadataQuery mq) {
        return computeCost(rel, mq);
    }

    /** */
    public RelOptCost getCumulativeRewindCost(IgniteCorrelatedNestedLoopJoin rel, RelMetadataQuery mq) {
        return computeCost(rel, mq);
    }

    /** */
    public RelOptCost getCumulativeRewindCost(IgniteSpool rel, RelMetadataQuery mq) {
        return rel.computeRewindCost(rel.getCluster().getPlanner(), mq);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost recursiveCost(RelNode rel, RelMetadataQuery mq) {
        return cumulativeRewindCost(rel, mq);
    }
}
