/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsResultRowCount;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.matchesOnce;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class CompactedIntArrayTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testCompactedIntArray() {
        int size = 1000;
        for (int bitsPerItem = 1; bitsPerItem < 31; bitsPerItem++) {

            ColocationGroup.CompactedIntArray.Builder builder = new ColocationGroup.CompactedIntArray.Builder(bitsPerItem, size);

            for (int i = 0; i < 1000; i++)
                builder.add(i & ~(-1 << bitsPerItem));

            ColocationGroup.CompactedIntArray arr = builder.build();

            assertEquals(size, arr.size());

            GridIntIterator iter = arr.iterator();

            for (int i = 0; i < size; i++) {
                assertTrue(iter.hasNext());
                assertEquals(i & ~(-1 << bitsPerItem), iter.next());
            }
        }
    }
}
