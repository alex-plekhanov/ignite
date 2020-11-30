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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 *
 */
public class PageLruListTest extends GridCommonAbstractTest {
    /** Max pages count. */
    private static final int MAX_PAGES_CNT = 100;

    /** Memory provider. */
    private static DirectMemoryProvider provider;

    /** Memory region. */
    private static DirectMemoryRegion region;

    /** We don't need page content in this test, only page system header will be enough. */
    private static final int pageSize = PageMemoryImpl.PAGE_OVERHEAD;

    /** LRU list. */
    PageLruList lru;

    /** Test watcher. */
    @Rule public TestRule testWatcher = new TestWatcher() {
        @Override protected void failed(Throwable e, Description description) {
            dump();
        }
    };

    /** */
    @BeforeClass
    public static void setUp() {
        provider = new UnsafeMemoryProvider(log);
        provider.initialize(new long[] {pageSize * MAX_PAGES_CNT});

        region = provider.nextRegion();
    }

    /** */
    @Before
    public void reinitTest() {
        GridUnsafe.setMemory(region.address(), pageSize * MAX_PAGES_CNT, (byte)0);

        lru = null;
    }

    /** */
    @AfterClass
    public static void tearDown() {
        provider.shutdown(true);
    }

    /** */
    @Test
    public void testAdd() {
        // Check start with probationary page.
        lru = new PageLruList(MAX_PAGES_CNT);

        addToTail(0, false);
        assertProbationarySegment(0);
        assertProtectedSegment();

        addToTail(1, true);
        assertProbationarySegment(0);
        assertProtectedSegment(1);

        addToTail(2, false);
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1);

        addToTail(3, true);
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1, 3);

        // Check start with protected page.
        reinitTest();
        lru = new PageLruList(MAX_PAGES_CNT);

        addToTail(0, true);
        assertProbationarySegment();
        assertProtectedSegment(0);

        addToTail(1, false);
        assertProbationarySegment(1);
        assertProtectedSegment(0);

        addToTail(2, true);
        assertProbationarySegment(1);
        assertProtectedSegment(0, 2);

        addToTail(3, false);
        assertProbationarySegment(1, 3);
        assertProtectedSegment(0, 2);
    }

    /** */
    @Test
    public void testRemove() {
        lru = new PageLruList(MAX_PAGES_CNT);

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);
        addToTail(4, false);
        addToTail(5, true);

        remove(0); // Head.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(1, 3, 5);

        remove(5); // Tail.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(1, 3);

        remove(1); // Protected segment head.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(3);

        remove(4); // Probationary segment tail.
        assertProbationarySegment(2);
        assertProtectedSegment(3);

        remove(2); // Last probationary segment item.
        assertProbationarySegment();
        assertProtectedSegment(3);

        remove(3); // Last potected segment and LRU item.
        assertProbationarySegment();
        assertProtectedSegment();

        addToTail(2, false);
        addToTail(3, true);

        remove(3); // Last protected segment item.
        assertProbationarySegment(2);
        assertProtectedSegment();

        remove(2); // Last probationary segment and LRU item.
        assertProbationarySegment();
        assertProtectedSegment();
    }

    /** */
    @Test
    public void testPoll() {
        lru = new PageLruList(MAX_PAGES_CNT);

        assertEquals(-1, poll());

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);

        assertEquals(0, poll());
        assertEquals(2, poll());
        assertEquals(1, poll());
        assertEquals(3, poll());
        assertEquals(-1, poll());
    }

    /** */
    @Test
    public void testMoveToTail() {
        lru = new PageLruList(MAX_PAGES_CNT);

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);

        moveToTail(3, true); // Move from protected segment head to protected segment head.
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1, 3);

        moveToTail(2, false); // Move from probationary segment head to probationary segment head.
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1, 3);

        moveToTail(2, true); // Move from probationary segment to protected segment head.
        assertProbationarySegment(0);
        assertProtectedSegment(1, 3, 2);

        moveToTail(1, false); // Move from protected segment to probationary segment head.
        assertProbationarySegment(0, 1);
        assertProtectedSegment(3, 2);

        moveToTail(3, true); // Move from protected segment to protected segment head.
        assertProbationarySegment(0, 1);
        assertProtectedSegment(2, 3);

        moveToTail(0, false); // Move from probationary segment to probationary segment head.
        assertProbationarySegment(1, 0);
        assertProtectedSegment(2, 3);
    }

    /** */
    @Test
    public void testProtectedToProbationaryMigration() {
        lru = new PageLruList(10);

        assertEquals(3, lru.protectedPagesLimit());

        addToTail(0, true);
        addToTail(1, true);
        addToTail(2, true);

        assertProbationarySegment();
        assertProtectedSegment(0, 1, 2);

        addToTail(3, true);
        assertProbationarySegment(0);
        assertProtectedSegment(1, 2, 3);

        addToTail(4, true);
        assertProbationarySegment(0, 1);
        assertProtectedSegment(2, 3, 4);
    }

    /**
     * Absolute page address by index.
     *
     * @param idx Index.
     */
    private long pagePtr(int idx) {
        return region.address() + idx * pageSize;
    }

    /**
     * Page index by absolute address.
     *
     * @param ptr Absolute address.
     */
    private int pageIdx(long ptr) {
        return ptr == 0L ? -1 : (int)((ptr - region.address()) / pageSize);
    }

    /** */
    private void addToTail(int pageIdx, boolean protectedPage) {
        lru.addToTail(pagePtr(pageIdx), protectedPage);

        checkInvariants();
    }

    /** */
    private void remove(int pageIdx) {
        lru.remove(pagePtr(pageIdx));

        checkInvariants();
    }

    /** */
    private int poll() {
        int idx = pageIdx(lru.poll());

        checkInvariants();

        return idx;
    }

    /** */
    private void moveToTail(int pageIdx, boolean protectedPage) {
        lru.moveToTail(pagePtr(pageIdx), protectedPage);

        checkInvariants();
    }

    /** */
    private void assertProbationarySegment(int ...pageIdxs) {
        assertTrue((lru.probTailPtr() != 0L) ^ F.isEmpty(pageIdxs));

        long curPtr = lru.headPtr();

        for (int pageIdx : pageIdxs) {
            assertEquals(pageIdx, pageIdx(curPtr));

            curPtr = (curPtr == lru.probTailPtr()) ? 0L : PageHeader.nextLruPage(curPtr);
        }

        if (!F.isEmpty(pageIdxs))
            assertTrue(curPtr == 0L);
    }

    /** */
    private void assertProtectedSegment(int ...pageIdxs) {
        long curPtr = lru.headPtr();

        if (lru.probTailPtr() != 0L)
            curPtr = PageHeader.nextLruPage(lru.probTailPtr());

        assertTrue((curPtr != 0L) ^ F.isEmpty(pageIdxs));

        for (int pageIdx : pageIdxs) {
            assertEquals(pageIdx, pageIdx(curPtr));

            curPtr = PageHeader.nextLruPage(curPtr);
        }

        assertEquals(pageIdxs.length, lru.protectedPagesCount());
    }

    /**
     * Check LRU list invariants.
     */
    private void checkInvariants() {
        int limit = MAX_PAGES_CNT + 1;

        long curPtr = lru.headPtr();
        int protectedCnt = 0;
        boolean protectedPage = (lru.probTailPtr() == 0L);

        if (lru.headPtr() == 0L || lru.tailPtr() == 0L) {
            assertEquals(0L, lru.headPtr());
            assertEquals(0L, lru.tailPtr());
            assertEquals(0L, lru.probTailPtr());
            assertEquals(0, lru.protectedPagesCount());
        }

        while (curPtr != 0L && limit-- > 0) {
            long prev = PageHeader.prevLruPage(curPtr);
            long next = PageHeader.nextLruPage(curPtr);

            if (prev == 0L)
                assertEquals(lru.headPtr(), curPtr);
            else
                assertEquals(curPtr, PageHeader.nextLruPage(prev));

            if (next == 0L)
                assertEquals(lru.tailPtr(), curPtr);
            else
                assertEquals(curPtr, PageHeader.prevLruPage(next));

            assertEquals(protectedPage, PageHeader.protectedLru(curPtr));

            if (protectedPage)
                protectedCnt++;

            if (curPtr == lru.probTailPtr())
                protectedPage = true;

            curPtr = next;
        }

        assertTrue(limit > 0);

        assertEquals(protectedCnt, lru.protectedPagesCount());
        assertTrue(protectedCnt <= lru.protectedPagesLimit());
    }

    /**
     * Dump LRU list content.
     */
    private void dump() {
        int limit = MAX_PAGES_CNT;

        log.info(String.format("LRU list dump [headPtr=%d, probTailPtr=%d, tailPtr=%d, protectedCnt=%d]",
            pageIdx(lru.headPtr()), pageIdx(lru.probTailPtr()), pageIdx(lru.tailPtr()), lru.protectedPagesCount()));

        long curPtr = lru.headPtr();

        while (curPtr != 0L && limit-- > 0) {
            log.info(String.format("    Page %d [prev=%d, next=%d, protected=%b]%s", pageIdx(curPtr),
                pageIdx(PageHeader.prevLruPage(curPtr)), pageIdx(PageHeader.nextLruPage(curPtr)),
                PageHeader.protectedLru(curPtr), curPtr == lru.probTailPtr() ? " <- probationary list tail" : ""));

            curPtr = PageHeader.nextLruPage(curPtr);
        }

        if (limit == 0)
            log.info("...");
    }
}