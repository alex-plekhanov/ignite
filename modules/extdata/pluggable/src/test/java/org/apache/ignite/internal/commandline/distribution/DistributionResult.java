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

package org.apache.ignite.internal.commandline.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** Distribution command result. */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
@GridInternal
public class DistributionResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Map<String, NodeInfo> nodesInfo;

    /** */
    public DistributionResult() {
        // No-op.
    }

    /** */
    public DistributionResult(Map<String, NodeInfo> nodesInfo) {
        this.nodesInfo = nodesInfo;
    }

    /** */
    public Map<String, NodeInfo> nodesInfo() {
        return nodesInfo;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, nodesInfo);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodesInfo = U.readMap(in);
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public static class NodeInfo extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String consistentId;

        /** */
        private Diff diff = Diff.REMAINED;

        /** */
        private Map<String, CacheInfo> cachesInfo;

        /** */
        public NodeInfo() {
            // No-op.
        }

        /** */
        public NodeInfo(String consistentId, Map<String, CacheInfo> cachesInfo) {
            this.consistentId = consistentId;
            this.cachesInfo = cachesInfo;
        }

        /** */
        public Map<String, CacheInfo> cachesInfo() {
            return cachesInfo;
        }

        /** */
        public String consistentId() {
            return consistentId;
        }

        /** */
        public Diff diff() {
            return diff;
        }

        /** */
        public NodeInfo diff(Diff diff) {
            this.diff = diff;

            return this;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeString(out, consistentId);
            out.writeByte(diff.ordinal());
            U.writeMap(out, cachesInfo);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            consistentId = U.readString(in);
            diff = Diff.fromOrdinal(in.readByte());
            cachesInfo = U.readMap(in);
        }
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public static class CacheInfo extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private Collection<String> cacheNames;

        /** */
        private int parts;

        /** */
        private Map<Integer, PartInfo> partsInfo;

        /** */
        private long idxSize;

        /** */
        private double idxToDataRatio;

        /** */
        private CacheMode cacheMode;

        /** */
        private boolean persistent;

        /** */
        public CacheInfo() {
            // No-op.
        }

        /** */
        public CacheInfo(
            Collection<String> cacheNames,
            CacheMode cacheMode,
            boolean persistent,
            int parts,
            Map<Integer, PartInfo> partsInfo,
            long idxSize,
            double idxToDataRatio
        ) {
            this.cacheNames = cacheNames;
            this.cacheMode = cacheMode;
            this.persistent = persistent;
            this.parts = parts;
            this.partsInfo = partsInfo;
            this.idxSize = idxSize;
            this.idxToDataRatio = idxToDataRatio;
        }

        /** */
        public int parts() {
            return parts;
        }

        /** */
        public boolean persistent() {
            return persistent;
        }

        /** */
        public CacheMode cacheMode() {
            return cacheMode;
        }

        /** */
        public Map<Integer, PartInfo> partsInfo() {
            return partsInfo;
        }

        /** */
        public long indexSize() {
            return idxSize;
        }

        /** */
        public double indexToDataRation() {
            return idxToDataRatio;
        }

        /** */
        public Collection<String> cacheNames() {
            return cacheNames;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeCollection(out, cacheNames);
            out.writeByte(cacheMode.code());
            out.writeBoolean(persistent);
            out.writeInt(parts);
            U.writeIntKeyMap(out, partsInfo);
            out.writeLong(idxSize);
            out.writeDouble(idxToDataRatio);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            cacheNames = U.readList(in);
            cacheMode = CacheMode.fromCode(in.readByte());
            persistent = in.readBoolean();
            parts = in.readInt();
            partsInfo = U.readIntKeyMap(in);
            idxSize = in.readLong();
            idxToDataRatio = in.readDouble();
        }
    }

    /** */
    public static class PartInfo extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int partId;

        /** */
        private Diff diff = Diff.REMAINED;

        /** */
        private String state;

        /** */
        private long size;

        /** */
        public PartInfo() {
            // No-op.
        }

        /** */
        public PartInfo(int partId, String state, long size) {
            this.partId = partId;
            this.state = state;
            this.size = size;
        }

        /** */
        public int partId() {
            return partId;
        }

        /** */
        public Diff diff() {
            return diff;
        }

        /** */
        public PartInfo diff(Diff diff) {
            this.diff = diff;

            return this;
        }

        /** */
        public String state() {
            return state;
        }

        /** */
        public long size() {
            return size;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            out.writeInt(partId);
            out.writeByte(diff.ordinal());
            U.writeString(out, state);
            out.writeLong(size);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
            partId = in.readInt();
            diff = Diff.fromOrdinal(in.readByte());
            state = U.readString(in);
            size = in.readLong();
        }
    }

    /** */
    public enum Diff {
        /** */ REMAINED(""),
        /** */ ADDED("+"),
        /** */ REMOVED("-");

        /** */
        private final String sign;

        /** */
        Diff(String sign) {
            this.sign = sign;
        }

        /** */
        public String sign() {
            return sign;
        }

        /** Enumerated values. */
        private static final Diff[] VALS = values();

        /** */
        @Nullable public static Diff fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}
