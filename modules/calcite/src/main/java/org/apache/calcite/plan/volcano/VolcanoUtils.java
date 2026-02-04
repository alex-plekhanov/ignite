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

package org.apache.calcite.plan.volcano;

import java.io.PrintWriter;
import java.util.Comparator;
import com.google.common.collect.Ordering;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class VolcanoUtils {
    /**
     * @param relSubset Subset.
     * @return Cost of best known plan.
     */
    public static RelOptCost bestCost(RelSubset relSubset) {
        return relSubset.bestCost;
    }

    /**
     * @param planner Planner.
     * @param pw Print writter.
     * @param cls Classes to dump.
     */
    public static void dump(VolcanoPlanner planner, PrintWriter pw, Class<? extends RelNode> cls) {
        Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
        for (RelSet set : ordering.immutableSortedCopy(planner.allSets)) {
            pw.println("Set#" + set.id + ", type: " + set.subsets.get(0).getRowType());

/*
            for (RelNode rel : set.rels) {
                if (cls.isInstance(rel)) {
                    pw.print("\t" + rel);
                    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
                    pw.print(", rowcount=" + mq.getRowCount(rel));
                    pw.println(", cumulative cost=" + planner.getCost(rel, mq));
                }
            }

            if (1 == 1)
                continue;
*/

            for (RelSubset subset : set.subsets) {
                if (F.find(subset.getRels(), null, cls::isInstance) == null)
                    continue;

                pw.println("\t" + subset + ", best=" + ((subset.best == null) ? "null" : ("rel#" + subset.best.getId())));

                for (RelNode rel : subset.getRels()) {
                    if (!cls.isInstance(rel))
                        continue;

                    // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
                    pw.print("\t\t" + rel);

                    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
                    pw.print(", rowcount=" + mq.getRowCount(rel));
                    pw.println(", cumulative cost=" + planner.getCost(rel, mq));
                }
            }
        }
    }

    /**
     * @param planner Planner.
     * @param pw Print writter.
     */
    public static void dump(VolcanoPlanner planner, PrintWriter pw) {
        Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
        for (RelSet set : ordering.immutableSortedCopy(planner.allSets)) {
            pw.println("Set#" + set.id + ", type: " + set.subsets.get(0).getRowType());

            pw.println("SubSets:");
            for (RelSubset subset : set.subsets)
                pw.println("\t" + subset + ", best=" + ((subset.best == null) ? "null" : ("rel#" + subset.best.getId())));

            pw.println("Rels:");
            for (RelNode rel : set.rels) {
                pw.print("\t" + rel);
                RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
                pw.print(", rowcount=" + mq.getRowCount(rel));
                pw.println(", cumulative cost=" + planner.getCost(rel, mq));
            }
        }
    }
}
