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

/**
 * Pages Segmented-LRU (SLRU) list implementation.
 *
 * SLRU list is divided into two segments, a probationary segment and a protected segment. Pages in each segment
 * are ordered from the least to the most recently accessed. New pages are added to the most recently accessed end
 * (tail) of the probationary segment. Existing pages are removed from wherever they currently reside and added to
 * the most recently accessed end of the protected segment. Pages in the protected segment have thus been accessed at
 * least twice. The protected segment is finite, so migration of a page from the probationary segment to the protected
 * segment may force the migration of the LRU page in the protected segment to the most recently used end of
 * the probationary segment, giving this page another chance to be accessed before being replaced.
 * Page to replace is polled from the least recently accessed end (head) of the probationary segment.
 */
public class PageLruList {
    /** Ratio to limit count of protected pages. */
    private static final double PROTECTED_TO_TOTAL_PAGES_RATIO = 0.3;

    /** Pointer to the head of LRU list. */
    private long headPtr;

    /** Pointer to the tail of LRU list. */
    private long tailPtr;

    /** Pointer to the tail of probationary segment. */
    private long probTailPtr;

    /** Count of protected pages in the list. */
    private int protectedPagesCnt;

    /** Protected pages segment limit. */
    private final int protectedPagesLimit;

    /**
     * @param totalPagesCnt Total pages count.
     */
    PageLruList(int totalPagesCnt) {
        protectedPagesLimit = (int)(totalPagesCnt * PROTECTED_TO_TOTAL_PAGES_RATIO);
    }

    /**
     * Remove page from the head of LRU list.
     *
     * @return Absolute pointer of the page or {@code 0L} if list is empty.
     */
    public synchronized long poll() {
        long ptr = headPtr;

        if (ptr != 0L)
            remove(ptr);

        return ptr;
    }

    /**
     * Remove page from LRU list by absolute pointer.
     *
     * @param absPtr Page absolute pointer.
     */
    public synchronized void remove(long absPtr) {
        assert absPtr != 0L;

        long prevAbsPtr = PageHeader.prevLruPage(absPtr);
        long nextAbsPtr = PageHeader.nextLruPage(absPtr);

        if (absPtr == probTailPtr)
            probTailPtr = prevAbsPtr;

        if (prevAbsPtr == 0L) {
            assert headPtr == absPtr : "Unexpected LRU pointer [headPtr=" + headPtr + ", absPtr=" + absPtr + ']';

            headPtr = nextAbsPtr;
        }
        else
            PageHeader.nextLruPage(prevAbsPtr, nextAbsPtr);

        if (nextAbsPtr == 0L) {
            assert tailPtr == absPtr : "Unexpected LRU pointer [tailPtr=" + tailPtr + ", absPtr=" + absPtr + ']';

            tailPtr = prevAbsPtr;
        }
        else
            PageHeader.prevLruPage(nextAbsPtr, prevAbsPtr);

        PageHeader.prevLruPage(absPtr, 0L);
        PageHeader.nextLruPage(absPtr, 0L);

        if (PageHeader.protectedLru(absPtr)) {
            protectedPagesCnt--;

            PageHeader.protectedLru(absPtr, false);
        }
    }

    /**
     * Add page to the tail of protected or probationary LRU list.
     *
     * @param absPtr Page absolute pointer.
     * @param protectedPage Protected page flag.
     */
    public synchronized void addToTail(long absPtr, boolean protectedPage) {
        assert PageHeader.prevLruPage(absPtr) == 0L : PageHeader.prevLruPage(absPtr);
        assert PageHeader.nextLruPage(absPtr) == 0L : PageHeader.nextLruPage(absPtr);

        if (headPtr == 0L || tailPtr == 0L) {
            // In case of empty list.
            assert headPtr == 0L : headPtr;
            assert tailPtr == 0L : tailPtr;
            assert probTailPtr == 0L : probTailPtr;
            assert protectedPagesCnt == 0 : protectedPagesCnt;

            headPtr = absPtr;
            tailPtr = absPtr;

            if (protectedPage) {
                protectedPagesCnt = 1;

                PageHeader.protectedLru(absPtr, true);
            }
            else
                probTailPtr = absPtr;

            return;
        }

        if (protectedPage) {
            // Protected page - insert to the list tail.
            assert PageHeader.nextLruPage(tailPtr) == 0L : "Unexpected LRU pointer [absPtr=" + absPtr +
                ", tailPtr=" + tailPtr + ", nextLruPtr=" + PageHeader.nextLruPage(tailPtr) + ']';

            link(tailPtr, absPtr);

            tailPtr = absPtr;

            PageHeader.protectedLru(absPtr, true);

            // Move one page from protected segment to probationary segment if there are too many protected pages.
            if (protectedPagesCnt >= protectedPagesLimit) {
                probTailPtr = probTailPtr != 0L ? PageHeader.nextLruPage(probTailPtr) : headPtr;

                assert probTailPtr != 0L;

                PageHeader.protectedLru(probTailPtr, false);
            }
            else
                protectedPagesCnt++;
        }
        else {
            if (probTailPtr == 0L) {
                // First page in the probationary list - insert to the head.
                assert PageHeader.prevLruPage(headPtr) == 0L : "Unexpected LRU pointer [absPtr=" + absPtr +
                    ", headPtr=" + headPtr + ", prevLruPtr=" + PageHeader.prevLruPage(headPtr) + ']';

                link(absPtr, headPtr);

                headPtr = absPtr;
            }
            else {
                long protectedPtr = PageHeader.nextLruPage(probTailPtr);

                link(probTailPtr, absPtr);

                if (protectedPtr == 0L) {
                    // There are no protected pages in the list.
                    assert probTailPtr == tailPtr :
                        "Unexpected LRU pointer [probTailPtr=" + probTailPtr + ", tailPtr=" + tailPtr + ']';

                    tailPtr = absPtr;
                }
                else {
                    // Link with last protected page.
                    link(absPtr, protectedPtr);
                }
            }

            probTailPtr = absPtr;
        }
    }

    /**
     * Move page to the tail of protected or probationary LRU list.
     *
     * @param absPtr Page absolute pointer.
     * @param protectedPage Protected page flag.
     */
    public synchronized void moveToTail(long absPtr, boolean protectedPage) {
        if ((protectedPage && tailPtr == absPtr) || (!protectedPage && probTailPtr == absPtr))
            return;

        remove(absPtr);
        addToTail(absPtr, protectedPage);
    }

    /**
     * Link two pages.
     *
     * @param prevAbsPtr Previous page absolute pointer.
     * @param nextAbsPtr Next page absolute pointer.
     */
    private void link(long prevAbsPtr, long nextAbsPtr) {
        PageHeader.prevLruPage(nextAbsPtr, prevAbsPtr);
        PageHeader.nextLruPage(prevAbsPtr, nextAbsPtr);
    }

    /**
     * Gets the pointer to the head of LRU list.
     */
    synchronized long headPtr() {
        return headPtr;
    }

    /**
     * Gets the pointer to the tail of probationary segment.
     */
    synchronized long probTailPtr() {
        return probTailPtr;
    }

    /**
     * Gets the pointer to the tail of LRU list.
     */
    synchronized long tailPtr() {
        return tailPtr;
    }

    /**
     * Gets protected pages count.
     */
    int protectedPagesCount() {
        return protectedPagesCnt;
    }

    /**
     * Gets protected pages limit.
     */
    int protectedPagesLimit() {
        return protectedPagesLimit;
    }
}
