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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;

/**
 * Cache data manipulation request.
 */
class ClientCacheDataRequest extends ClientCacheRequest {
    /** Transaction ID. Only available if request was made under a transaction. */
    private final int txId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientCacheDataRequest(BinaryRawReader reader) {
        super(reader);

        txId = isTransactional() ? reader.readInt() : 0;
    }

    /**
     * Gets transaction ID.
     */
    public int txId() {
        return txId;
    }

    /** {@inheritDoc} */
    @Override public boolean isTransactional() {
        return super.isTransactional();
    }

    /**
     * Read cache object from stream, including RAW bytes.
     */
    protected static CacheObject readCacheObject(BinaryRawReaderEx reader, BinaryInputStream in, boolean isKey) {
        int pos0 = in.position();

        Object obj = reader.readObjectDetached();

        int pos1 = in.position();

        in.position(pos0);

        byte[] objBytes = in.readByteArray(pos1 - pos0);

        return isKey ? new KeyCacheObjectImpl(obj, objBytes, -1) : new CacheObjectImpl(obj, objBytes);
    }
}
