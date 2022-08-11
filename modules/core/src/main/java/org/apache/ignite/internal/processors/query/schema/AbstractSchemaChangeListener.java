package org.apache.ignite.internal.processors.query.schema;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Abstract schema change listener with no-op implementation for all calbacks.
 */
public class AbstractSchemaChangeListener implements SchemaChangeListener {
    /** {@inheritDoc} */
    @Override public void onSchemaCreated(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDropped(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexCreated(
        String schemaName,
        String tblName,
        String idxName,
        IndexDescriptor idxDesc
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        boolean isSql
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onColumnsAdded(String schemaName, String tblName, List<QueryField> cols, boolean ifColNotExists) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onColumnsDropped(String schemaName, String tblName, List<String> cols, boolean ifColExists) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDescriptor,
        boolean destroy,
        boolean clearIdx
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
        // No-op.
    }
}
