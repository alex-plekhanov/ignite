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

package org.apache.ignite.internal.processors.query;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.managers.systemview.walker.SqlIndexViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlSchemaViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewViewWalker;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.schema.AbstractSchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.sql.SqlIndexView;
import org.apache.ignite.spi.systemview.view.sql.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableColumnView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableView;
import org.apache.ignite.spi.systemview.view.sql.SqlViewColumnView;
import org.apache.ignite.spi.systemview.view.sql.SqlViewView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.query.QueryUtils.matches;

/**
 * Schema manager. Responsible for all manipulations on schema objects.
 */
public class GridQuerySchemaManagerImpl implements GridQuerySchemaManager {
    /** */
    public static final String SQL_SCHEMA_VIEW = "schemas";

    /** */
    public static final String SQL_SCHEMA_VIEW_DESC = "SQL schemas";

    /** */
    public static final String SQL_TBLS_VIEW = "tables";

    /** */
    public static final String SQL_TBLS_VIEW_DESC = "SQL tables";

    /** */
    public static final String SQL_VIEWS_VIEW = "views";

    /** */
    public static final String SQL_VIEWS_VIEW_DESC = "SQL views";

    /** */
    public static final String SQL_IDXS_VIEW = "indexes";

    /** */
    public static final String SQL_IDXS_VIEW_DESC = "SQL indexes";

    /** */
    public static final String SQL_TBL_COLS_VIEW = metricName("table", "columns");

    /** */
    public static final String SQL_TBL_COLS_VIEW_DESC = "SQL table columns";

    /** */
    public static final String SQL_VIEW_COLS_VIEW = metricName("view", "columns");

    /** */
    public static final String SQL_VIEW_COLS_VIEW_DESC = "SQL view columns";

    /** */
    private final SchemaChangeListener lsnr;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, GridQuerySchema> schemas = new ConcurrentHashMap<>();

    /** Cache name -> schema name */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

    /** Map from table identifier to table. */
    private final ConcurrentMap<T2<String, String>, GridQueryTable> id2tbl = new ConcurrentHashMap<>();

    /** System VIEW collection. */
    private final Set<SystemView<?>> sysViews = new GridConcurrentHashSet<>();

    /** Mutex to synchronize schema operations. */
    private final Object schemaMux = new Object();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public GridQuerySchemaManagerImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        lsnr = schemaChangeListener(ctx);
        log = ctx.log(GridQuerySchemaManagerImpl.class);

        ctx.systemView().registerView(SQL_SCHEMA_VIEW, SQL_SCHEMA_VIEW_DESC,
            new SqlSchemaViewWalker(),
            schemas.values(),
            SqlSchemaView::new);

        ctx.systemView().registerView(SQL_TBLS_VIEW, SQL_TBLS_VIEW_DESC,
            new SqlTableViewWalker(),
            id2tbl.values(),
            SqlTableView::new);

        ctx.systemView().registerView(SQL_VIEWS_VIEW, SQL_VIEWS_VIEW_DESC,
            new SqlViewViewWalker(),
            sysViews,
            SqlViewView::new);

        ctx.systemView().registerInnerCollectionView(SQL_IDXS_VIEW, SQL_IDXS_VIEW_DESC,
            new SqlIndexViewWalker(),
            id2tbl.values(),
            t -> t.descriptor().indexes().values(), // TODO check PK indexes and proxies
            SqlIndexView::new);

        ctx.systemView().registerInnerCollectionView(SQL_TBL_COLS_VIEW, SQL_TBL_COLS_VIEW_DESC,
            new SqlTableColumnViewWalker(),
            id2tbl.values(),
            t -> t.descriptor().properties().values(),
            SqlTableColumnView::new); // TODO check _KEY, _VAL columns

        ctx.systemView().registerInnerCollectionView(SQL_VIEW_COLS_VIEW, SQL_VIEW_COLS_VIEW_DESC,
            new SqlViewColumnViewWalker(),
            sysViews,
            v -> MetricUtils.systemViewAttributes(v).entrySet(),
            SqlViewColumnView::new);
    }

    /**
     * Handle node start.
     *
     * @param schemaNames Schema names.
     */
    public void start(String[] schemaNames) throws IgniteCheckedException {
        // Register PUBLIC schema which is always present.
        schemas.put(QueryUtils.DFLT_SCHEMA, new GridQuerySchema(QueryUtils.DFLT_SCHEMA, true));

        // Create schemas listed in node's configuration.
        createPredefinedSchemas(schemaNames);
    }

    /**
     * Handle node stop.
     */
    public void stop() {
        schemas.clear();
        cacheName2schema.clear();
    }

    /**
     * Registers new system view.
     *
     * @param schema Schema to create view in.
     * @param view System view.
     */
    public void createSystemView(String schema, SystemView<?> view) {
        boolean disabled = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

        if (disabled) {
            if (log.isInfoEnabled()) {
                log.info("SQL system views will not be created because they are disabled (see " +
                    IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS + " system property)");
            }

            return;
        }

        try {
            synchronized (schemaMux) {
                createSchema(schema, true);
            }

            sysViews.add(view);

            lsnr.onSystemViewCreated(schema, view);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to register system view.", e);
        }
    }

    /**
     * Create predefined schemas.
     *
     * @param schemaNames Schema names.
     */
    private void createPredefinedSchemas(String[] schemaNames) throws IgniteCheckedException {
        if (F.isEmpty(schemaNames))
            return;

        Collection<String> schemaNames0 = new LinkedHashSet<>();

        for (String schemaName : schemaNames) {
            if (F.isEmpty(schemaName))
                continue;

            schemaName = QueryUtils.normalizeSchemaName(null, schemaName);

            schemaNames0.add(schemaName);
        }

        synchronized (schemaMux) {
            for (String schemaName : schemaNames0)
                createSchema(schemaName, true);
        }
    }

    /**
     * Invoked when cache is created.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param sqlFuncs Custom SQL functions.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheCreated(String cacheName, String schemaName, Class<?>[] sqlFuncs) throws IgniteCheckedException {
        synchronized (schemaMux) {
            createSchema(schemaName, false);
        }

        cacheName2schema.put(cacheName, schemaName);

        createSqlFunctions(schemaName, sqlFuncs);
    }

    /**
     * Registers new class description.
     *
     * @param cacheInfo Cache info.
     * @param type Type descriptor.
     * @param isSql Whether SQL enabled.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheTypeCreated(
        GridCacheContextInfo<?, ?> cacheInfo,
        GridQueryTypeDescriptor type,
        boolean isSql
    ) throws IgniteCheckedException {
        String schemaName = schemaName(cacheInfo.name());

        GridQuerySchema schema = schema(schemaName);

        lsnr.onSqlTypeCreated(schemaName, type, cacheInfo, isSql);

        GridQueryTable tbl = new GridQueryTable(cacheInfo, type);

        createSystemIndexes(tbl);
        createInitialUserIndexes(tbl);

        schema.add(tbl);

        T2<String, String> tableId = new T2<>(schemaName, type.tableName());

        if (id2tbl.putIfAbsent(tableId, tbl) != null)
            throw new IllegalStateException("Table already exists: " + schemaName + '.' + type.tableName());
    }

    /**
     * Handle cache destroy.
     *
     * @param cacheName Cache name.
     * @param rmvIdx Whether to remove indexes.
     * @param clearIdx Whether to clear the index.
     */
    public void onCacheDestroyed(String cacheName, boolean rmvIdx, boolean clearIdx) {
        String schemaName = schemaName(cacheName);

        GridQuerySchema schema = schemas.get(schemaName);

        // Remove this mapping only after callback to DML proc - it needs that mapping internally.
        cacheName2schema.remove(cacheName);

        for (GridQueryTable tbl : schema.tables()) {
            if (F.eq(tbl.cacheInfo().name(), cacheName)) {
                try {
                    lsnr.onSqlTypeDropped(schemaName, tbl.descriptor(), rmvIdx, clearIdx);
                }
                catch (Exception e) {
                    U.error(log, "Failed to drop table on cache stop (will ignore): " +
                        tbl.descriptor().tableName(), e);
                }

                schema.drop(tbl);

                T2<String, String> tableId = new T2<>(tbl.cacheInfo().name(), tbl.descriptor().tableName());
                id2tbl.remove(tableId, tbl);
            }
        }

        synchronized (schemaMux) {
            if (schema.decrementUsageCount()) {
                schemas.remove(schemaName);

                try {
                    lsnr.onSchemaDropped(schemaName);
                }
                catch (Exception e) {
                    U.error(log, "Failed to drop schema on cache stop (will ignore): " + cacheName, e);
                }
            }
        }
    }

    /**
     * Create and register schema if needed.
     *
     * @param schemaName Schema name.
     * @param predefined Whether this is predefined schema.
     */
    private void createSchema(String schemaName, boolean predefined) throws IgniteCheckedException {
        assert Thread.holdsLock(schemaMux);

        if (!predefined)
            predefined = isSchemaPredefined(schemaName);

        GridQuerySchema schema = new GridQuerySchema(schemaName, predefined);

        GridQuerySchema oldSchema = schemas.putIfAbsent(schemaName, schema);

        if (oldSchema == null)
            lsnr.onSchemaCreated(schemaName);
        else
            schema = oldSchema;

        schema.incrementUsageCount();
    }

    /**
     * Check if schema is predefined.
     *
     * @param schemaName Schema name.
     * @return {@code True} if predefined.
     */
    private boolean isSchemaPredefined(String schemaName) {
        return F.eq(QueryUtils.DFLT_SCHEMA, schemaName);
    }

    /**
     * Registers SQL functions.
     *
     * @param schema Schema.
     * @param clss Classes.
     * @throws IgniteCheckedException If failed.
     */
    private void createSqlFunctions(String schema, Class<?>[] clss) throws IgniteCheckedException {
        if (F.isEmpty(clss))
            return;

        for (Class<?> cls : clss) {
            for (Method m : cls.getDeclaredMethods()) {
                QuerySqlFunction ann = m.getAnnotation(QuerySqlFunction.class);

                if (ann != null) {
                    int modifiers = m.getModifiers();

                    if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers))
                        throw new IgniteCheckedException("Method " + m.getName() + " must be public static.");

                    String alias = ann.alias().isEmpty() ? m.getName() : ann.alias();

                    lsnr.onFunctionCreated(schema, alias, ann.deterministic(), m);
                }
            }
        }
    }

    /**
     * Get schema name for cache.
     *
     * @param cacheName Cache name.
     * @return Schema name.
     */
    public String schemaName(String cacheName) {
        String res = cacheName2schema.get(cacheName);

        if (res == null)
            res = "";

        return res;
    }

    /**
     * Get schemas names.
     *
     * @return Schemas names.
     */
    public Set<String> schemaNames() {
        return new HashSet<>(schemas.keySet());
    }

    /**
     * Get schema by name.
     *
     * @param schemaName Schema name.
     * @return Schema.
     */
    private GridQuerySchema schema(String schemaName) {
        return schemas.get(schemaName);
    }

    /**
     * Create system indexes for table.
     *
     * @param tbl Table.
     */
    private void createSystemIndexes(GridQueryTable tbl) {
        List<GridQueryIndexDescriptor> sysIdxs = new ArrayList<>();

        //TODO
        //sysIdxs.add(new QuerySysIndexDescriptorImpl(idxName, idxCols));

        for (GridQueryIndexDescriptor idxDesc : sysIdxs) {
            Index idx = null; // TODO

            lsnr.onIndexCreated(tbl.descriptor().schemaName(), tbl.descriptor().tableName(), idxDesc.name(), idxDesc,
                idx);
        }
    }

    /**
     * Create initial user indexes.
     *
     * @param tbl Table.
     */
    private void createInitialUserIndexes(GridQueryTable tbl) {
        for (GridQueryIndexDescriptor idxDesc : tbl.descriptor().indexes().values()) {
            Index idx = null; // TODO

            lsnr.onIndexCreated(tbl.descriptor().schemaName(), tbl.descriptor().tableName(), idxDesc.name(), idxDesc,
                idx);
        }
    }

    /**
     * Create index dynamically.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxDesc Index descriptor.
     * @param ifNotExists If-not-exists.
     * @param cacheVisitor Cache visitor.
     * @throws IgniteCheckedException If failed.
     */
    public void createIndex(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc, boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
        // Locate table.
        GridQuerySchema schema = schema(schemaName);

        GridQueryTable tbl = (schema != null ? schema.tableByName(tblName) : null);

        if (tbl == null) {
            throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');
        }

        Index idx = null; // TODO

        lsnr.onIndexCreated(schemaName, tbl.descriptor().tableName(), idxDesc.name(), idxDesc, idx);
    }

    /**
     * Creates index dynamically.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param h2Idx Index.
     * @param ifNotExists If-not-exists.
     * @throws IgniteCheckedException If failed.
     */
    public void createIndex(String schemaName, String tblName, Index h2Idx, boolean ifNotExists)
        throws IgniteCheckedException {
        // Locate table.
        GridQuerySchema schema = schema(schemaName);

        GridQueryTable tbl = (schema != null ? schema.tableByName(tblName) : null);

        if (tbl == null) {
            throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');
        }

        Index idx = null; // TODO
        GridQueryIndexDescriptor idxDesc = null; // TODO

        // TODO it's added
        lsnr.onIndexCreated(schemaName, tbl.descriptor().tableName(), idxDesc.name(), idxDesc, idx);
    }

    /**
     * Drop index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists If exists.
     */
    public void dropIndex(final String schemaName, String idxName, boolean ifExists) {
        GridQueryTypeDescriptor desc = typeDescriptorForIndex(schemaName, idxName);

        if (desc != null)
            lsnr.onIndexDropped(schemaName, desc.tableName(), idxName);
    }

    /**
     * Add column.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns.
     * @param ifTblExists If table exists.
     * @param ifColNotExists If column not exists.
     * @throws IgniteCheckedException If failed.
     */
    public void addColumn(String schemaName, String tblName, List<QueryField> cols,
        boolean ifTblExists, boolean ifColNotExists) throws IgniteCheckedException {
        // Locate table.
        GridQuerySchema schema = schema(schemaName);

        GridQueryTable tbl = (schema != null ? schema.tableByName(tblName) : null);

        if (tbl == null) {
            if (!ifTblExists) {
                throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                    ", tblName=" + tblName + ']');
            }
            else
                return;
        }

        lsnr.onColumnsAdded(schemaName, tblName, cols, ifColNotExists);
    }

    /**
     * Drop column.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns.
     * @param ifTblExists If table exists.
     * @param ifColExists If column exists.
     * @throws IgniteCheckedException If failed.
     */
    public void dropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException {
        // Locate table.
        GridQuerySchema schema = schema(schemaName);

        GridQueryTable tbl = (schema != null ? schema.tableByName(tblName) : null);

        if (tbl == null) {
            if (!ifTblExists) {
                throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                    ",tblName=" + tblName + ']');
            }
            else
                return;
        }

        lsnr.onColumnsDropped(schemaName, tblName, cols, ifColExists);
    }

    /**
     * Get table descriptor.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Descriptor.
     */
    @Nullable public GridQueryTypeDescriptor tableForType(String schemaName, String cacheName, String type) {
        GridQuerySchema schema = schema(schemaName);

        if (schema == null)
            return null;

        GridQueryTable tbl = schema.tableByTypeName(cacheName, type);

        return tbl == null ? null : tbl.descriptor();
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param cacheName Cache name.
     * @return Collection of table descriptors.
     */
    public Collection<GridQueryTypeDescriptor> tablesForCache(String cacheName) {
        GridQuerySchema schema = schema(schemaName(cacheName));

        if (schema == null)
            return Collections.emptySet();

        List<GridQueryTypeDescriptor> tbls = new ArrayList<>();

        for (GridQueryTable tbl : schema.tables()) {
            if (F.eq(tbl.cacheInfo().name(), cacheName))
                tbls.add(tbl.descriptor());
        }

        return tbls;
    }

    /**
     * Find table by it's identifier.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Table or {@code null} if none found.
     */
    public GridQueryTable dataTable(String schemaName, String tblName) {
        return id2tbl.get(new T2<>(schemaName, tblName));
    }

    /**
     * @return all known tables.
     */
    public Collection<GridQueryTable> dataTables() {
        return id2tbl.values();
    }

    /**
     * @return all known system views.
     */
    public Collection<SystemView<?>> systemViews() {
        return Collections.unmodifiableSet(sysViews);
    }

    /**
     * Find table for index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @return Table or {@code null} if index is not found.
     */
    public GridQueryTypeDescriptor dataTableForIndex(String schemaName, String idxName) {
        for (Map.Entry<T2<String, String>, GridQueryTable> dataTableEntry : id2tbl.entrySet()) {
            if (F.eq(dataTableEntry.getKey().get1(), schemaName)) {
                GridQueryTypeDescriptor desc = dataTableEntry.getValue().descriptor();

                if (desc.indexes().containsKey(idxName))
                    return desc;
            }
        }

        return null;
    }

    /**
     * Mark tables for index rebuild, so that their indexes are not used.
     *
     * @param cacheName Cache name.
     * @param mark Mark/unmark flag, {@code true} if index rebuild started, {@code false} if finished.
     */
    public void markIndexRebuild(String cacheName, boolean mark) {
        for (GridQueryTypeDescriptor tblDesc : tablesForCache(cacheName)) {
            if (mark)
                lsnr.onIndexRebuildStarted(tblDesc.schemaName(), tblDesc.tableName());
            else
                lsnr.onIndexRebuildFinished(tblDesc.schemaName(), tblDesc.tableName());
        }
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName) {
        GridQueryTable tbl = dataTable(schemaName, tableName);
        return tbl == null ? null : tbl.descriptor();
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName) {
        return dataTableForIndex(schemaName, idxName);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName) {
        GridQueryTable tbl = id2tbl.get(new T2<>(schemaName, tableName));

        return tbl == null ? null : (GridCacheContextInfo<K, V>)tbl.cacheInfo();
    }

    /**
     * Return table information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param tblTypes Filter by table type. As Of now supported only 'TABLES' and 'VIEWS'.
     * Can be {@code null} or empty to don't use the filter.
     *
     * @return Table information filtered by given patterns.
     */
    public Collection<TableInformation> tablesInformation(
        String schemaNamePtrn,
        String tblNamePtrn,
        String... tblTypes
    ) {
        Set<String> types = F.isEmpty(tblTypes) ? Collections.emptySet() : new HashSet<>(Arrays.asList(tblTypes));

        Collection<TableInformation> infos = new ArrayList<>();

        boolean allTypes = F.isEmpty(tblTypes);

        if (allTypes || types.contains(JdbcUtils.TYPE_TABLE)) {
            dataTables().stream()
                .filter(t -> matches(t.descriptor().schemaName(), schemaNamePtrn))
                .filter(t -> matches(t.descriptor().tableName(), tblNamePtrn))
                .map(t -> {
                    int cacheGrpId = t.cacheInfo().groupId();

                    CacheGroupDescriptor cacheGrpDesc = ctx.cache().cacheGroupDescriptors().get(cacheGrpId);

                    // We should skip table in case regarding cache group has been removed.
                    if (cacheGrpDesc == null)
                        return null;

                    GridQueryTypeDescriptor type = t.descriptor();

                    return new TableInformation(t.descriptor().schemaName(), t.descriptor().tableName(),
                        JdbcUtils.TYPE_TABLE, cacheGrpId, cacheGrpDesc.cacheOrGroupName(), t.cacheInfo().cacheId(),
                        t.cacheInfo().name(), type.affinityKey(), type.keyFieldAlias(), type.valueFieldAlias(),
                        type.keyTypeName(), type.valueTypeName());
                })
                .filter(Objects::nonNull)
                .forEach(infos::add);
        }

        if ((allTypes || types.contains(JdbcUtils.TYPE_VIEW)) && matches(QueryUtils.SCHEMA_SYS, schemaNamePtrn)) {
            systemViews().stream()
                .filter(t -> matches(MetricUtils.toSqlName(t.name()), tblNamePtrn))
                .map(v -> new TableInformation(QueryUtils.SCHEMA_SYS, MetricUtils.toSqlName(v.name()), JdbcUtils.TYPE_VIEW))
                .forEach(infos::add);
        }

        return infos;
    }

    /**
     * Return column information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param colNamePtrn Filter by column name. Can be {@code null} to don't use the filter.
     *
     * @return Column information filtered by given patterns.
     */
    public Collection<ColumnInformation> columnsInformation(
        String schemaNamePtrn,
        String tblNamePtrn,
        String colNamePtrn
    ) {
        Collection<ColumnInformation> infos = new ArrayList<>();

        // Gather information about tables.
        dataTables().stream()
            .filter(t -> matches(t.descriptor().schemaName(), schemaNamePtrn))
            .filter(t -> matches(t.descriptor().tableName(), tblNamePtrn))
            .flatMap(
                tbl -> {
                    int[] cnt = new int[1];

                    return tbl.descriptor().fields().keySet().stream()
                        .filter(field -> matches(field, colNamePtrn))
                        .map(field -> {
                            GridQueryProperty prop = tbl.descriptor().property(field);

                            return new ColumnInformation(
                                ++cnt[0], // Start from 1.
                                tbl.descriptor().schemaName(),
                                tbl.descriptor().tableName(),
                                field,
                                prop.type(),
                                !prop.notNull(),
                                prop.defaultValue(),
                                prop.precision(),
                                prop.scale(),
                                field.equals(tbl.descriptor().affinityKey()));
                        });
                }
            ).forEach(infos::add);

        // Gather information about system views.
        if (matches(QueryUtils.SCHEMA_SYS, schemaNamePtrn)) {
            systemViews().stream()
                .filter(v -> matches(MetricUtils.toSqlName(v.name()), tblNamePtrn))
                .flatMap(
                    view -> {
                        int[] cnt = new int[1];

                        return MetricUtils.systemViewAttributes(view).entrySet().stream()
                            .filter(c -> matches(MetricUtils.toSqlName(c.getKey()), colNamePtrn))
                            .map(c -> new ColumnInformation(
                                ++cnt[0], // Start from 1.
                                QueryUtils.SCHEMA_SYS,
                                MetricUtils.toSqlName(view.name()),
                                c.getKey(),
                                c.getValue(),
                                true,
                                null,
                                -1,
                                -1,
                                false)
                            );
                    }
                ).forEach(infos::add);
        }

        return infos;
    }

    /** */
    private SchemaChangeListener schemaChangeListener(GridKernalContext ctx) {
        List<SchemaChangeListener> subscribers = new ArrayList<>(ctx.internalSubscriptionProcessor().getSchemaChangeSubscribers());

        if (F.isEmpty(subscribers))
            return new NoOpSchemaChangeListener();

        return subscribers.size() == 1 ? subscribers.get(0) : new CompoundSchemaChangeListener(subscribers);
    }

    /** */
    private static final class NoOpSchemaChangeListener extends AbstractSchemaChangeListener {
        // No-op.
    }

    /** */
    private static final class CompoundSchemaChangeListener implements SchemaChangeListener {
        /** */
        private final List<SchemaChangeListener> lsnrs;

        /**
         * @param lsnrs Lsnrs.
         */
        private CompoundSchemaChangeListener(List<SchemaChangeListener> lsnrs) {
            this.lsnrs = lsnrs;
        }

        /** {@inheritDoc} */
        @Override public void onSchemaCreated(String schemaName) {
            lsnrs.forEach(lsnr -> lsnr.onSchemaCreated(schemaName));
        }

        /** {@inheritDoc} */
        @Override public void onSchemaDropped(String schemaName) {
            lsnrs.forEach(lsnr -> lsnr.onSchemaDropped(schemaName));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeCreated(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            boolean isSql
        ) {
            lsnrs.forEach(lsnr -> lsnr.onSqlTypeCreated(schemaName, typeDesc, cacheInfo, isSql));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsAdded(String schemaName, String tblName, List<QueryField> cols, boolean ifColNotExists) {
            lsnrs.forEach(lsnr -> lsnr.onColumnsAdded(schemaName, tblName, cols, ifColNotExists));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsDropped(String schemaName, String tblName, List<String> cols, boolean ifColExists) {
            lsnrs.forEach(lsnr -> lsnr.onColumnsDropped(schemaName, tblName, cols, ifColExists));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDescriptor,
            boolean destroy,
            boolean clearIdx
        ) {
            lsnrs.forEach(lsnr -> lsnr.onSqlTypeDropped(schemaName, typeDescriptor, destroy, clearIdx));
        }

        /** {@inheritDoc} */
        @Override public void onIndexCreated(String schemaName, String tblName, String idxName,
            GridQueryIndexDescriptor idxDesc, Index idx) {
            lsnrs.forEach(lsnr -> lsnr.onIndexCreated(schemaName, tblName, idxName, idxDesc, idx));
        }

        /** {@inheritDoc} */
        @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
            lsnrs.forEach(lsnr -> lsnr.onIndexDropped(schemaName, tblName, idxName));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> lsnr.onIndexRebuildStarted(schemaName, tblName));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> lsnr.onIndexRebuildFinished(schemaName, tblName));
        }

        /** {@inheritDoc} */
        @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method) {
            lsnrs.forEach(lsnr -> lsnr.onFunctionCreated(schemaName, name, deterministic, method));
        }

        /** {@inheritDoc} */
        @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
            lsnrs.forEach(lsnr -> lsnr.onSystemViewCreated(schemaName, sysView));
        }
    }
}
