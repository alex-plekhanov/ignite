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

package org.apache.ignite.internal.processors.query.h2.views;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;

/**
 * System views processor.
 */
public class GridH2SysViewProcessor {
    /** Logger. */
    private volatile IgniteLogger log;

    /** Registered views. */
    private final Collection<GridH2SysView> registeredViews = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param idx indexing.
     */
    public void start(GridKernalContext ctx, IgniteH2Indexing idx) {
        log = ctx.log(GridH2SysViewProcessor.class);
        log.info("Starting system view processor");

        Connection c = idx.connectionForSchema(GridH2SysView.TABLE_SCHEMA_NAME);

        try {
            Collection<GridH2SysView> viewsToRegister = new ArrayList<>();

            viewsToRegister.add(new GridH2SysViewImplViews(ctx, this));
            viewsToRegister.add(new GridH2SysViewImplInstance(ctx));
            viewsToRegister.add(new GridH2SysViewImplJvmThreads(ctx));
            viewsToRegister.add(new GridH2SysViewImplJvmRuntime(ctx));
            viewsToRegister.add(new GridH2SysViewImplJvmOS(ctx));
            viewsToRegister.add(new GridH2SysViewImplCaches(ctx));
            viewsToRegister.add(new GridH2SysViewImplCacheGroups(ctx));
            viewsToRegister.add(new GridH2SysViewImplNodes(ctx));
            viewsToRegister.add(new GridH2SysViewImplNodeAttributes(ctx));
            viewsToRegister.add(new GridH2SysViewImplNodeMetrics(ctx));

            for (GridH2SysView view : viewsToRegister) {
                GridH2SysViewTableEngine.registerView(c, view);

                registeredViews.add(view);

                if (log.isDebugEnabled())
                    log.debug("Registered system view: " + view.getTableName());
            }
        }
        catch (SQLException e) {
            log.error("Failed to register system views: ", e);

            throw new IgniteException(e);
        }
    }

    /**
     * Gets registered views.
     */
    public Collection<GridH2SysView> getRegisteredViews() {
        return registeredViews;
    }
}
