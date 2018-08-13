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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.lang.IgniteParentChildIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * System view: node attributes.
 */
public class SqlSystemViewNodeAttributes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewNodeAttributes(GridKernalContext ctx) {
        super("NODE_ATTRIBUTES", "Node attributes", ctx, new String[] {"NODE_ID,NAME", "NAME"},
            newColumn("NODE_ID", Value.UUID),
            newColumn("NAME"),
            newColumn("VALUE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Collection<ClusterNode> nodes;

        SqlSystemViewColumnCondition idCond = conditionForColumn("NODE_ID", first, last);
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        if (idCond.isEquality()) {
            try {
                UUID nodeId = uuidFromValue(idCond.valueForEquality());

                ClusterNode node = ctx.discovery().node(nodeId);

                if (node != null)
                    nodes = Collections.singleton(node);
                else
                    nodes = Collections.emptySet();
            }
            catch (Exception e) {
                log.warning("Failed to get node by nodeId: " + idCond.valueForEquality().getString(), e);

                nodes = Collections.emptySet();
            }
        }
        else
            nodes = F.concat(false, ctx.discovery().allNodes(), ctx.discovery().daemonNodes());

        if (nameCond.isEquality()) {
            String attrName = nameCond.valueForEquality().getString();

            List<Row> rows = new ArrayList<>();

            for (ClusterNode node : nodes) {
                if (node.attributes().containsKey(attrName))
                    rows.add(
                        createRow(ses, rows.size(),
                            node.id(),
                            attrName,
                            node.attribute(attrName)
                        )
                    );
            }

            return rows.iterator();
        }
        else {
            return new ParentChildRowIterator<>(ses, nodes.iterator(),
                new IgniteClosure<ClusterNode, Iterator<Map.Entry<String, Object>>>() {
                    @Override public Iterator<Map.Entry<String, Object>> apply(ClusterNode node) {
                        return node.attributes().entrySet().iterator();
                    }
                },
                new IgniteBiClosure<ClusterNode, Map.Entry<String, Object>, Object[]>() {
                    @Override public Object[] apply(ClusterNode node, Map.Entry<String, Object> attr) {
                        return new Object[] {
                            node.id(),
                            attr.getKey(),
                            attr.getValue()
                        };
                    }
                });
        }
    }

    /**
     * Parent-child Row iterator.
     *
     * @param <P> Parent class.
     * @param <C> Child class
     */
    private class ParentChildRowIterator<P, C> extends IgniteParentChildIterator<P, C, Row> {
        /**
         * @param ses Session.
         * @param parentIter Parent iterator.
         * @param cloChildIter Child iterator closure.
         * @param cloResFromParentChild Row columns from parent and child closure.
         */
        ParentChildRowIterator(final Session ses, Iterator<P> parentIter,
            IgniteClosure<P, Iterator<C>> cloChildIter,
            final IgniteBiClosure<P, C, Object[]> cloResFromParentChild) {
            super(parentIter, cloChildIter, new IgniteBiClosure<P, C, Row>() {
                /** Row count. */
                private int rowCnt = 0;

                @Override public Row apply(P p, C c) {
                    return SqlSystemViewNodeAttributes.this.createRow(ses, ++rowCnt, cloResFromParentChild.apply(p, c));
                }
            });
        }
    }
}
