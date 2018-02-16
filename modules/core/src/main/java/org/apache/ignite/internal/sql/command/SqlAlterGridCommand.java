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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;

import static org.apache.ignite.internal.sql.SqlKeyword.CANCEL;
import static org.apache.ignite.internal.sql.SqlKeyword.TASK;
import static org.apache.ignite.internal.sql.SqlKeyword.TRANSACTION;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * ALTER GRID command.
 */
public class SqlAlterGridCommand implements SqlCommand {
    /** Id to cancel. */
    private String idToCancel;

    /** Entity to cancel. */
    private String entityToCancel;

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        skipIfMatchesKeyword(lex, CANCEL);

        SqlLexerToken tok = lex.lookAhead();

        if (matchesKeyword(tok, TRANSACTION) || matchesKeyword(tok, TASK))
            entityToCancel = tok.token();
        else
            throw errorUnexpectedToken(lex, TRANSACTION, TASK);

        lex.shift();

        if (lex.shift())
            idToCancel = lex.token();

        return this;
    }

    /**
     * @return Id to cancel
     * .
     */
    public String idToCancel() {
        return idToCancel;
    }

    /**
     * @return Entity to cancel.
     */
    public String entityToCancel() {
        return entityToCancel;
    }
}
