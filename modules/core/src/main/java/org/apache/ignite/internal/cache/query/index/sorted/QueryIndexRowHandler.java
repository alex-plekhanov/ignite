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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;

/** Maps CacheDataRow to IndexRow using columns references. */
public class QueryIndexRowHandler implements InlineIndexRowHandler {
    /** Cache descriptor. */
    private final GridQueryRowDescriptor rowDescriptor;

    /** Index to columns references. */
    private volatile List<Integer> keyColumns;

    /** List of key types for inlined index keys. */
    private final List<InlineIndexKeyType> keyTypes;

    /** List of index key definitions. */
    private final List<IndexKeyDefinition> keyDefs;

    /** List of index key field names. */
    private final List<String> keyFieldNames;

    /** Index key type settings. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** */
    public QueryIndexRowHandler(
        GridQueryRowDescriptor rowDescriptor,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        List<InlineIndexKeyType> keyTypes,
        IndexKeyTypeSettings keyTypeSettings
    ) {
        this.keyTypes = Collections.unmodifiableList(keyTypes);
        this.keyDefs = Collections.unmodifiableList(new ArrayList<>(keyDefs.values()));
        keyFieldNames = Collections.unmodifiableList(new ArrayList<>(keyDefs.keySet()));

        this.rowDescriptor = rowDescriptor;
        this.keyTypeSettings = keyTypeSettings;

        onMetadataUpdated();
    }

    /** {@inheritDoc} */
    @Override public void onMetadataUpdated() {
        List<Integer> keyColumns = new ArrayList<>(keyFieldNames.size());

        for (String fieldName : keyFieldNames) {
            int colId = rowDescriptor.columnId(fieldName);

            assert colId >= 0 : "Unexpected field name [fieldName=" + fieldName + ", keyNames=" + keyFieldNames + ']';

            keyColumns.add(colId);
        }

        this.keyColumns = Collections.unmodifiableList(keyColumns);
    }

    /** {@inheritDoc} */
    @Override public IndexKey indexKey(int idx, CacheDataRow row) {
        Object o = getKey(idx, row);

        return IndexKeyFactory.wrap(
            o, keyDefs.get(idx).idxType(), rowDescriptor.context().cacheObjectContext(), keyTypeSettings);
    }

    /** {@inheritDoc} */
    @Override public List<InlineIndexKeyType> inlineIndexKeyTypes() {
        return keyTypes;
    }

    /** {@inheritDoc} */
    @Override public List<IndexKeyDefinition> indexKeyDefinitions() {
        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyTypeSettings indexKeyTypeSettings() {
        return keyTypeSettings;
    }

    /** */
    private Object getKey(int idx, CacheDataRow row) {
        int colId = keyColumns.get(idx);

        if (rowDescriptor.isKeyColumn(colId))
            return unwrap(row.key());

        else if (rowDescriptor.isValueColumn(colId))
            return unwrap(row.value());

        // getFieldValue ignores default columns (_KEY, _VAL), so make this shift.
        return rowDescriptor.getFieldValue(row.key(), row.value(), colId - QueryUtils.DEFAULT_COLUMNS_COUNT);
    }

    /** {@inheritDoc} */
    @Override public int partition(CacheDataRow row) {
        Object key = unwrap(row.key());

        return rowDescriptor.context().affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public Object cacheKey(CacheDataRow row) {
        return unwrap(row.key());
    }

    /** {@inheritDoc} */
    @Override public Object cacheValue(CacheDataRow row) {
        return unwrap(row.value());
    }

    /** */
    private Object unwrap(CacheObject val) {
        Object o = getBinaryObject(val);

        if (o != null)
            return o;

        CacheObjectContext coctx = rowDescriptor.context().cacheObjectContext();

        return val.value(coctx, false);
    }

    /** */
    private Object getBinaryObject(CacheObject o) {
        if (o instanceof BinaryObjectImpl) {
            ((BinaryObjectImpl)o).detachAllowed(true);
            o = ((BinaryObjectImpl)o).detach();
            return o;
        }

        return null;
    }
}
