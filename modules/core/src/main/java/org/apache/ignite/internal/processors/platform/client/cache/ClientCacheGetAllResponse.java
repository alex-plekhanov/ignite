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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.Map;

/**
 * GetAll response.
 */
class ClientCacheGetAllResponse extends ClientResponse {
    /** Result. */
    private final Map<Object, Object> res;

    /**
     * Ctor.
     *
     * @param requestId Request id.
     * @param res Result.
     */
    ClientCacheGetAllResponse(long requestId, Map<Object, Object> res) {
        super(requestId);

        assert res != null;

        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(res.size());

        for (Map.Entry<Object, Object> e : res.entrySet()) {
            try {
                writer.out().writeByteArray(((CacheObject)e.getKey()).valueBytes(null));
                writer.out().writeByteArray(((CacheObject)e.getValue()).valueBytes(null));
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
/*
            writer.writeObjectDetached(e.getKey());
            writer.writeObjectDetached(e.getValue());
*/
        }
    }
}
