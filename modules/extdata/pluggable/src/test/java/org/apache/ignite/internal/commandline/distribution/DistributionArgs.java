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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Distribution command arguments. */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class DistributionArgs extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names to process. */
    @GridToStringInclude
    private Set<String> caches;

    /** New baseline topology nodes consistent IDs with nodes attributes. */
    @GridToStringInclude
    private Map<String, Map<String, String>> baseline;

    /** */
    public DistributionArgs() {
        // No-op.
    }

    /** */
    public DistributionArgs(Set<String> caches, Map<String, Map<String, String>> baseline) {
        this.caches = caches;
        this.baseline = baseline;
    }

    /** @return Cache names to process. */
    public Set<String> caches() {
        return caches;
    }

    /** @return New baseline topology nodes consistent IDs. */
    public Map<String, Map<String, String>> baseline() {
        return baseline;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);
        U.writeMap(out, baseline);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);
        baseline = U.readHashMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributionArgs.class, this);
    }
}
