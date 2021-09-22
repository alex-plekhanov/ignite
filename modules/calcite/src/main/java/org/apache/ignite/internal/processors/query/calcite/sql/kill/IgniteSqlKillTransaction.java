/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.sql.kill;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlKill;

/**
 * Parse tree for {@code KILL TRANSACTION} statement.
 */
public class IgniteSqlKillTransaction extends IgniteSqlKill {
    /** */
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("KILL TRANSACTION", SqlKind.OTHER_DDL);

    /**  */
    private final SqlCharStringLiteral xid;

    /** */
    public IgniteSqlKillTransaction(
        SqlParserPos pos,
        SqlCharStringLiteral xid
    ) {
        super(OPERATOR, pos);
        this.xid = Objects.requireNonNull(xid, "xid");

    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(xid);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        xid.unparse(writer, 0, 0);
    }

    /**
     * @return Transaction's xid.
     */
    public SqlCharStringLiteral xid() {
        return xid;
    }
}
