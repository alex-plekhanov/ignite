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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

/**
 * Ignite RexBuilder.
 */
public class IgniteRexBuilder extends RexBuilder {
    /**
     * @param typeFactory Type factory.
     */
    public IgniteRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public RexLiteral makeCharLiteral(NlsString str) {
        assert str != null;

        Charset charset = str.getCharset();

        if (null == charset)
            charset = typeFactory.getDefaultCharset();

        SqlCollation collation = str.getCollation();

        if (null == collation)
            collation = SqlCollation.COERCIBLE;

        RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR, str.getValue().length());

        type = typeFactory.createTypeWithCharsetAndCollation(type, charset, collation);

        return makeLiteral(str, type, SqlTypeName.CHAR);
    }
}
