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

package org.apache.ignite.internal.processors.query.schema.management;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.Order;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.QueryIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.managers.systemview.walker.SqlIndexViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlSchemaViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewViewWalker;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.ColumnInformation;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQuerySchemaManager;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QuerySysIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.QueryUtils.KeyOrValProperty;
import org.apache.ignite.internal.processors.query.TableInformation;
import org.apache.ignite.internal.processors.query.schema.AbstractSchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
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
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.matches;

/**
 * Schema manager. Responsible for all manipulations on schema objects.
 */
public class SchemaManager implements GridQuerySchemaManager {
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
    private static final InlineIndexFactory SORTED_IDX_FACTORY = InlineIndexFactory.INSTANCE;

    /** */
    private static final Map<QueryIndexType, IndexDescriptorFactory>
        IDX_DESC_FACTORY = new EnumMap<>(QueryIndexType.class);

    /** */
    private volatile SchemaChangeListener lsnr;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, SchemaDescriptor> schemas = new ConcurrentHashMap<>();

    /** Cache name -> schema name */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

    /** Map from table identifier to table. */
    private final ConcurrentMap<T2<String, String>, TableDescriptor> id2tbl = new ConcurrentHashMap<>();

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
    public SchemaManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(SchemaManager.class);
    }

    /** Register index descriptor factory for custom index type. */
    public static void registerIndexDescriptorFactory(
        QueryIndexType type,
        IndexDescriptorFactory factory
    ) {
        IDX_DESC_FACTORY.put(type, factory);
    }

    /**
     * Handle node start.
     *
     * @param schemaNames Schema names.
     */
    public void start(String[] schemaNames) throws IgniteCheckedException {
        lsnr = schemaChangeListener(ctx);

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
            t -> t.indexes().values(),
            SqlIndexView::new);

        ctx.systemView().registerInnerCollectionView(SQL_TBL_COLS_VIEW, SQL_TBL_COLS_VIEW_DESC,
            new SqlTableColumnViewWalker(),
            id2tbl.values(),
            this::tableColumns,
            SqlTableColumnView::new);

        ctx.systemView().registerInnerCollectionView(SQL_VIEW_COLS_VIEW, SQL_VIEW_COLS_VIEW_DESC,
            new SqlViewColumnViewWalker(),
            sysViews,
            v -> MetricUtils.systemViewAttributes(v).entrySet(),
            SqlViewColumnView::new);

        // Register PUBLIC schema which is always present.
        synchronized (schemaMux) {
            createSchema(QueryUtils.DFLT_SCHEMA, true);
        }

        // Create schemas listed in node's configuration.
        createPredefinedSchemas(schemaNames);
    }

    /** */
    private Collection<GridQueryProperty> tableColumns(TableDescriptor tblDesc) {
        GridQueryTypeDescriptor typeDesc = tblDesc.descriptor();
        Collection<GridQueryProperty> props = typeDesc.properties().values();

        if (!tblDesc.descriptor().properties().containsKey(KEY_FIELD_NAME))
            props = F.concat(false, new KeyOrValProperty(true, KEY_FIELD_NAME, typeDesc.keyClass()), props);

        if (!tblDesc.descriptor().properties().containsKey(VAL_FIELD_NAME))
            props = F.concat(false, new KeyOrValProperty(false, VAL_FIELD_NAME, typeDesc.valueClass()), props);

        return props;
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
        validateTypeDescriptor(type);

        String schemaName = schemaName(cacheInfo.name());

        SchemaDescriptor schema = schema(schemaName);

        lsnr.onSqlTypeCreated(schemaName, type, cacheInfo);

        TableDescriptor tbl = new TableDescriptor(cacheInfo, type, isSql);

        createSystemIndexes(tbl);
        createInitialUserIndexes(tbl);

        schema.add(tbl);

        T2<String, String> tableId = new T2<>(schemaName, type.tableName());

        if (id2tbl.putIfAbsent(tableId, tbl) != null)
            throw new IllegalStateException("Table already exists: " + schemaName + '.' + type.tableName());
    }

    /**
     * Validates properties described by query types.
     *
     * @param type Type descriptor.
     * @throws IgniteCheckedException If validation failed.
     */
    private static void validateTypeDescriptor(GridQueryTypeDescriptor type) throws IgniteCheckedException {
        assert type != null;

        Collection<String> names = new HashSet<>(type.fields().keySet());

        if (names.size() < type.fields().size())
            throw new IgniteCheckedException("Found duplicated properties with the same name [keyType=" +
                type.keyClass().getName() + ", valueType=" + type.valueClass().getName() + "]");

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [type=" + type.name() + "]";

        for (String name : names) {
            if (name.equalsIgnoreCase(KEY_FIELD_NAME) || name.equalsIgnoreCase(VAL_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }
    }

    /**
     * Handle cache destroy.
     *
     * @param cacheName Cache name.
     * @param rmvIdx Whether to remove indexes.
     * @param clearIdx Whether to clear the index.
     */
    public void onCacheDestroyed(String cacheName, boolean rmvIdx, boolean clearIdx) throws IgniteCheckedException {
        String schemaName = schemaName(cacheName);

        SchemaDescriptor schema = schemas.get(schemaName);

        // Remove this mapping only after callback to DML proc - it needs that mapping internally.
        cacheName2schema.remove(cacheName);

        for (TableDescriptor tbl : schema.tables()) {
            if (F.eq(tbl.cacheInfo().name(), cacheName)) {
                Collection<IndexDescriptor> idxs = new ArrayList<>(tbl.indexes().values());

                for (IndexDescriptor idx : idxs) {
                    if (!idx.isProxy()) // Proxies will be deleted implicitly after deleting target index.
                        dropIndex(tbl, idx.name(), true, !clearIdx);
                }

                try {
                    lsnr.onSqlTypeDropped(schemaName, tbl.descriptor(), rmvIdx);
                }
                catch (Exception e) {
                    U.error(log, "Failed to drop table on cache stop (will ignore): " +
                        tbl.descriptor().tableName(), e);
                }

                schema.drop(tbl);

                T2<String, String> tableId = new T2<>(tbl.descriptor().schemaName(), tbl.descriptor().tableName());
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

        SchemaDescriptor schema = new SchemaDescriptor(schemaName, predefined);

        SchemaDescriptor oldSchema = schemas.putIfAbsent(schemaName, schema);

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
    private SchemaDescriptor schema(String schemaName) {
        return schemas.get(schemaName);
    }

    /**
     * Create system indexes for table.
     *
     * @param tbl Table.
     */
    private void createSystemIndexes(TableDescriptor tbl) {
        createPkIndex(tbl);
        createAffinityIndex(tbl);
    }

    /** */
    private void createPkIndex(TableDescriptor tbl) {
        GridQueryIndexDescriptor idxDesc = new QuerySysIndexDescriptorImpl(QueryUtils.PRIMARY_KEY_INDEX,
            Collections.emptyList(), tbl.descriptor().primaryKeyInlineSize()); // _KEY field will be added implicitly.

        // Add primary key index.
        createIndexDescriptor(
            idxDesc,
            tbl,
            true,
            false,
            null
        );
    }

    /** */
    private void createAffinityIndex(TableDescriptor tbl) {
        // Locate index where affinity column is first (if any).
        if (tbl.affinityKey() != null) {
            boolean affIdxFound = false;

            for (GridQueryIndexDescriptor idxDesc : tbl.descriptor().indexes().values()) {
                if (idxDesc.type() != QueryIndexType.SORTED)
                    continue;

                affIdxFound |= F.eq(tbl.affinityKey(), F.first(idxDesc.fields()));
            }

            // Add explicit affinity key index if nothing alike was found.
            if (!affIdxFound) {
                GridQueryIndexDescriptor idxDesc = new QuerySysIndexDescriptorImpl(QueryUtils.AFFINITY_KEY_INDEX,
                    Collections.singleton(tbl.affinityKey()), tbl.descriptor().affinityFieldInlineSize());

                createIndexDescriptor(
                    idxDesc,
                    tbl,
                    false,
                    true,
                    null
                );
            }
        }
    }

    /** */
    private IndexDescriptor createIndexDescriptor(
        GridQueryIndexDescriptor idxDesc,
        TableDescriptor tbl,
        boolean isPk,
        boolean isAffKey,
        @Nullable SchemaIndexCacheVisitor cacheVisitor
    ) {
        IndexDescriptor desc;

        if (idxDesc.type() == QueryIndexType.SORTED)
            desc = createSortedIndexDescriptor(idxDesc, tbl, isPk, isAffKey, cacheVisitor);
        else {
            if (IDX_DESC_FACTORY.containsKey(idxDesc.type()))
                desc = IDX_DESC_FACTORY.get(idxDesc.type()).create(idxDesc, tbl);
            else
                throw new IllegalStateException("Index type: " + idxDesc.type());
        }

        addIndex(tbl, desc);

        return desc;
    }

    /** Create proxy index for real index if needed. */
    private IndexDescriptor createProxyIndexDescriptor(
        IndexDescriptor idxDesc,
        TableDescriptor tbl
    ) {
        GridQueryTypeDescriptor typeDesc = tbl.descriptor();
        if (F.isEmpty(typeDesc.keyFieldName()) && F.isEmpty(typeDesc.valueFieldName()))
            return null;

        String keyAlias = typeDesc.keyFieldAlias();
        String valAlias = typeDesc.valueFieldAlias();

        LinkedHashMap<String, IndexKeyDefinition> proxyKeyDefs = new LinkedHashMap<>(idxDesc.keyDefinitions().size());

        boolean modified = false;

        for (Map.Entry<String, IndexKeyDefinition> keyDef : idxDesc.keyDefinitions().entrySet()) {
            String oldFieldName = keyDef.getKey();
            String newFieldName = oldFieldName;

            // Replace _KEY/_VAL field with aliases and vice versa.
            if (F.eq(oldFieldName, QueryUtils.KEY_FIELD_NAME) && !F.isEmpty(keyAlias))
                newFieldName = keyAlias;
            else if (F.eq(oldFieldName, QueryUtils.VAL_FIELD_NAME) && !F.isEmpty(valAlias))
                newFieldName = valAlias;
            else if (F.eq(oldFieldName, keyAlias))
                newFieldName = QueryUtils.KEY_FIELD_NAME;
            else if (F.eq(oldFieldName, valAlias))
                newFieldName = QueryUtils.VAL_FIELD_NAME;

            modified |= !F.eq(oldFieldName, newFieldName);

            proxyKeyDefs.put(newFieldName, keyDef.getValue());
        }

        if (!modified)
            return null;

        String proxyName = generateProxyIdxName(idxDesc.name());

        IndexDescriptor proxyDesc = new IndexDescriptor(proxyName, proxyKeyDefs, idxDesc);

        tbl.addIndex(proxyName, proxyDesc);

        lsnr.onIndexCreated(tbl.descriptor().schemaName(), tbl.descriptor().tableName(), proxyName, proxyDesc);

        return proxyDesc;
    }

    /** */
    public static String generateProxyIdxName(String idxName) {
        return idxName + "_proxy";
    }

    /**
     * Create sorted index.
     *
     * @param idxDesc Index descriptor,
     * @param tbl Table.
     * @param isPk Primary key flag.
     * @param isAffKey Affinity key flag.
     * @param cacheVisitor whether index created with new cache or on existing one.
     * @return Index descriptor.
     */
    private IndexDescriptor createSortedIndexDescriptor(
        GridQueryIndexDescriptor idxDesc,
        TableDescriptor tbl,
        boolean isPk,
        boolean isAffKey,
        @Nullable SchemaIndexCacheVisitor cacheVisitor
    ) {
        GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();
        GridQueryTypeDescriptor typeDesc = tbl.descriptor();
        String idxName = idxDesc.name();

        if (log.isDebugEnabled())
            log.debug("Creating cache index [cacheId=" + cacheInfo.cacheId() + ", idxName=" + idxName + ']');

        LinkedHashMap<String, IndexKeyDefinition> originalIdxCols = indexDescriptorToKeysDefinition(idxDesc, typeDesc);
        LinkedHashMap<String, IndexKeyDefinition> wrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Enrich wrapped columns collection with key and affinity key.
        addKeyColumn(wrappedCols, tbl);
        addAffinityColumn(wrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> unwrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Enrich unwrapped columns collection with unwrapped key fields and affinity key.
        addUnwrappedKeyColumns(unwrappedCols, tbl);
        addAffinityColumn(unwrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> idxCols = unwrappedCols;

        Index idx;

        if (cacheInfo.affinityNode()) {
            GridCacheContext<?, ?> cctx = cacheInfo.cacheContext();

            int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

            String treeName = BPlusTree.treeName(typeId + "_" + idxName, "H2Tree");

            if (!ctx.indexProcessor().useUnwrappedPk(cctx, treeName))
                idxCols = wrappedCols;

            QueryIndexDefinition idxDef = new QueryIndexDefinition(
                typeDesc,
                cacheInfo,
                new IndexName(cacheInfo.name(), typeDesc.schemaName(), typeDesc.tableName(), idxName),
                treeName,
                ctx.indexProcessor().rowCacheCleaner(cacheInfo.groupId()),
                isPk,
                isAffKey,
                idxCols,
                idxDesc.inlineSize(),
                ctx.indexProcessor().keyTypeSettings()
            );

            if (cacheVisitor != null)
                idx = ctx.indexProcessor().createIndexDynamically(cctx, SORTED_IDX_FACTORY, idxDef, cacheVisitor);
            else
                idx = ctx.indexProcessor().createIndex(cctx, SORTED_IDX_FACTORY, idxDef);
        }
        else {
            ClientIndexDefinition d = new ClientIndexDefinition(
                new IndexName(tbl.cacheInfo().name(), tbl.descriptor().schemaName(), tbl.descriptor().tableName(), idxName),
                idxCols,
                idxDesc.inlineSize(),
                tbl.cacheInfo().config().getSqlIndexMaxInlineSize());

            idx = ctx.indexProcessor().createIndex(tbl.cacheInfo().cacheContext(), new ClientIndexFactory(log), d);
        }

        assert idx instanceof InlineIndex : idx;

        return new IndexDescriptor(idxName, idxDesc.type(), idxCols, isPk, isAffKey, ((InlineIndex)idx).inlineSize(), idx);
    }

    /** Split key into simple components and add to columns list. */
    private static void addUnwrappedKeyColumns(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        // Key unwrapping possible only for SQL created tables.
        if (!tbl.isSql() || QueryUtils.isSqlType(tbl.descriptor().keyClass())) {
            addKeyColumn(cols, tbl);

            return;
        }

        int oldColsSize = cols.size();

        if (!tbl.descriptor().primaryKeyFields().isEmpty()) {
            for (String keyName : tbl.descriptor().primaryKeyFields())
                cols.put(keyName, keyDefinition(tbl.descriptor(), keyName, true));
        }
        else {
            for (String propName : tbl.descriptor().fields().keySet()) {
                GridQueryProperty prop = tbl.descriptor().property(propName);

                if (prop.key())
                    cols.put(propName, keyDefinition(tbl.descriptor(), propName, true));
            }
        }

        // If key is object but the user has not specified any particular columns,
        // we have to fall back to whole-key index.
        if (cols.size() == oldColsSize)
            addKeyColumn(cols, tbl);
    }

    /** Add key column, if it (or it's alias) wasn't added before. */
    private static void addKeyColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        if (!cols.containsKey(QueryUtils.KEY_FIELD_NAME)
            && (F.isEmpty(tbl.descriptor().keyFieldName()) || !cols.containsKey(tbl.descriptor().keyFieldAlias())))
            cols.put(QueryUtils.KEY_FIELD_NAME, keyDefinition(tbl.descriptor(), QueryUtils.KEY_FIELD_NAME, true));
    }

    /** Add affinity column, if it wasn't added before. */
    private static void addAffinityColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        if (tbl.affinityKey() != null)
            cols.put(tbl.affinityKey(), keyDefinition(tbl.descriptor(), tbl.affinityKey(), true));
    }

    /** */
    private static LinkedHashMap<String, IndexKeyDefinition> indexDescriptorToKeysDefinition(
        GridQueryIndexDescriptor idxDesc,
        GridQueryTypeDescriptor typeDesc
    ) {
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = new LinkedHashMap<>(idxDesc.fields().size());

        for (String field : idxDesc.fields())
            keyDefs.put(field, keyDefinition(typeDesc, field, !idxDesc.descending(field)));

        return keyDefs;
    }

    /** */
    private static IndexKeyDefinition keyDefinition(GridQueryTypeDescriptor typeDesc, String field, boolean ascOrder) {
        Order order = new Order(ascOrder ? SortOrder.ASC : SortOrder.DESC, null);

        GridQueryProperty prop = typeDesc.property(field);

        Class<?> fieldType = prop != null ? prop.type() :
            F.eq(field, QueryUtils.KEY_FIELD_NAME) ? typeDesc.keyClass() :
                F.eq(field, QueryUtils.VAL_FIELD_NAME) ? typeDesc.valueClass() : null;

        int fieldPrecession = prop != null ? prop.precision() : -1;

        return new IndexKeyDefinition(IndexKeyType.forClass(fieldType).code(), order, fieldPrecession);
    }

    /**
     * Create initial user indexes.
     *
     * @param tbl Table.
     */
    private void createInitialUserIndexes(TableDescriptor tbl) {
        for (GridQueryIndexDescriptor idxDesc : tbl.descriptor().indexes().values())
            createIndexDescriptor(idxDesc, tbl, false, false, null);
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
    public void createIndex(
        String schemaName,
        String tblName,
        QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor
    ) throws IgniteCheckedException {
        // Locate table.
        TableDescriptor tbl = dataTable(schemaName, tblName);

        if (tbl == null) {
            throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');
        }

        if (tbl.indexes().containsKey(idxDesc.name())) {
            if (ifNotExists)
                return;
            else
                throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_EXISTS, idxDesc.name());
        }

        createIndexDescriptor(idxDesc, tbl, false, false, cacheVisitor);
    }

    /**
     * Add index to the schema.
     *
     * @param tbl Table descriptor.
     * @param idxDesc Index descriptor.
     */
    public void addIndex(TableDescriptor tbl, IndexDescriptor idxDesc) {
        tbl.addIndex(idxDesc.name(), idxDesc);

        lsnr.onIndexCreated(tbl.descriptor().schemaName(), tbl.descriptor().tableName(), idxDesc.name(), idxDesc);

        createProxyIndexDescriptor(idxDesc, tbl);
    }

    /**
     * Drop index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists If exists.
     */
    public void dropIndex(final String schemaName, String idxName, boolean ifExists) throws IgniteCheckedException {
        GridQueryTypeDescriptor desc = typeDescriptorForIndex(schemaName, idxName);

        if (desc != null) {
            TableDescriptor tbl = dataTable(schemaName, desc.tableName());

            if (tbl == null)
                return;

            dropIndex(tbl, idxName, ifExists, false);
        }
    }

    /**
     * Drop index.
     *
     * @param tbl Table descriptor.
     * @param idxName Index name.
     * @param ifExists If exists.
     */
    private void dropIndex(TableDescriptor tbl, String idxName, boolean ifExists, boolean softDelete) throws IgniteCheckedException {
        String schemaName = tbl.descriptor().schemaName();
        String cacheName = tbl.descriptor().cacheName();
        String tableName = tbl.descriptor().tableName();

        IndexDescriptor idxDesc = tbl.dropIndex(idxName);

        if (idxDesc == null) {
            if (ifExists)
                return;
            else
                throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND, idxName);
        }

        if (!idxDesc.isProxy()) {
            ctx.indexProcessor().removeIndex(
                new IndexName(cacheName, schemaName, tableName, idxName),
                softDelete
            );
        }

        lsnr.onIndexDropped(schemaName, tableName, idxName);

        // Drop proxy for target index.
        for (IndexDescriptor proxyDesc : tbl.indexes().values()) {
            if (proxyDesc.targetIdx() == idxDesc) {
                tbl.dropIndex(proxyDesc.name());

                lsnr.onIndexDropped(schemaName, tableName, proxyDesc.name());
            }
        }
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
        TableDescriptor tbl = dataTable(schemaName, tblName);

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
        TableDescriptor tbl = dataTable(schemaName, tblName);

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
    @Nullable public GridQueryTypeDescriptor typeDescriptorForType(String schemaName, String cacheName, String type) {
        SchemaDescriptor schema = schema(schemaName);

        if (schema == null)
            return null;

        TableDescriptor tbl = schema.tableByTypeName(cacheName, type);

        return tbl == null ? null : tbl.descriptor();
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param cacheName Cache name.
     * @return Collection of table descriptors.
     */
    public Collection<GridQueryTypeDescriptor> typeDescriptorsForCache(String cacheName) {
        SchemaDescriptor schema = schema(schemaName(cacheName));

        if (schema == null)
            return Collections.emptySet();

        List<GridQueryTypeDescriptor> tbls = new ArrayList<>();

        for (TableDescriptor tbl : schema.tables()) {
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
    public TableDescriptor dataTable(String schemaName, String tblName) {
        return id2tbl.get(new T2<>(schemaName, tblName));
    }

    /**
     * @return all known tables.
     */
    public Collection<TableDescriptor> dataTables() {
        return id2tbl.values();
    }

    /**
     * Mark tables for index rebuild, so that their indexes are not used.
     *
     * @param cacheName Cache name.
     * @param mark Mark/unmark flag, {@code true} if index rebuild started, {@code false} if finished.
     */
    public void markIndexRebuild(String cacheName, boolean mark) {
        for (GridQueryTypeDescriptor typeDesc : typeDescriptorsForCache(cacheName)) {
            dataTable(typeDesc.schemaName(), typeDesc.tableName()).markIndexRebuildInProgress(mark);

            if (mark)
                lsnr.onIndexRebuildStarted(typeDesc.schemaName(), typeDesc.tableName());
            else
                lsnr.onIndexRebuildFinished(typeDesc.schemaName(), typeDesc.tableName());
        }
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName) {
        TableDescriptor tbl = dataTable(schemaName, tableName);
        return tbl == null ? null : tbl.descriptor();
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName) {
        for (Map.Entry<T2<String, String>, TableDescriptor> dataTableEntry : id2tbl.entrySet()) {
            if (F.eq(dataTableEntry.getKey().get1(), schemaName)) {
                GridQueryTypeDescriptor desc = dataTableEntry.getValue().descriptor();

                if (desc.indexes().containsKey(idxName))
                    return desc;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName) {
        TableDescriptor tbl = id2tbl.get(new T2<>(schemaName, tableName));

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
            sysViews.stream()
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
            sysViews.stream()
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

        return new CompoundSchemaChangeListener(ctx, subscribers);
    }

    /** Factory for custom type (except SORTED) index descriptors. */
    public interface IndexDescriptorFactory {
        /** */
        public IndexDescriptor create(GridQueryIndexDescriptor idxDesc, TableDescriptor tbl);
    }

    /** */
    private static final class NoOpSchemaChangeListener extends AbstractSchemaChangeListener {
        // No-op.
    }

    /** */
    private static final class CompoundSchemaChangeListener implements SchemaChangeListener {
        /** */
        private final List<SchemaChangeListener> lsnrs;

        /** */
        private final IgniteLogger log;

        /**
         * @param ctx Kernal context.
         * @param lsnrs Lsnrs.
         */
        private CompoundSchemaChangeListener(GridKernalContext ctx, List<SchemaChangeListener> lsnrs) {
            this.lsnrs = lsnrs;
            log = ctx.log(CompoundSchemaChangeListener.class);
        }

        /** {@inheritDoc} */
        @Override public void onSchemaCreated(String schemaName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSchemaCreated(schemaName)));
        }

        /** {@inheritDoc} */
        @Override public void onSchemaDropped(String schemaName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSchemaDropped(schemaName)));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeCreated(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSqlTypeCreated(schemaName, typeDesc, cacheInfo)));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsAdded(String schemaName, String tblName, List<QueryField> cols, boolean ifColNotExists) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onColumnsAdded(schemaName, tblName, cols, ifColNotExists)));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsDropped(String schemaName, String tblName, List<String> cols, boolean ifColExists) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onColumnsDropped(schemaName, tblName, cols, ifColExists)));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDescriptor,
            boolean destroy
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSqlTypeDropped(schemaName, typeDescriptor, destroy)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexCreated(
            String schemaName,
            String tblName,
            String idxName,
            IndexDescriptor idxDesc
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexCreated(schemaName, tblName, idxName, idxDesc)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexDropped(schemaName, tblName, idxName)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexRebuildStarted(schemaName, tblName)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexRebuildFinished(schemaName, tblName)));
        }

        /** {@inheritDoc} */
        @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onFunctionCreated(schemaName, name, deterministic, method)));
        }

        /** {@inheritDoc} */
        @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSystemViewCreated(schemaName, sysView)));
        }

        /** */
        private void executeSafe(Runnable r) {
            try {
                r.run();
            }
            catch (Exception e) {
                log.warning("Failed to notify listener (will ignore): " + e.getMessage(), e);
            }
        }
    }
}
