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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Ignite convertlet table.
 */
public class IgniteConvertletTable extends ReflectiveConvertletTable {
    /** Instance. */
    public static final IgniteConvertletTable INSTANCE = new IgniteConvertletTable();

    /** */
    private IgniteConvertletTable() {
        // Replace Calcite's convertlet with our own.
        registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF, new TimestampDiffConvertlet());
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public SqlRexConvertlet get(SqlCall call) {
        SqlRexConvertlet res = super.get(call);

        return res == null ? StandardConvertletTable.INSTANCE.get(call) : res;
    }

    /**
     * Converts a CASE expression.
     */
    public RexNode convertCase(SqlRexContext cx, SqlCase call) {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        assert whenList.size() == thenList.size();

        RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> exprList = new ArrayList<>();
        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        final RexLiteral unknownLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
        final RexLiteral nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));

        for (int i = 0; i < whenList.size(); i++) {
            if (SqlUtil.isNullLiteral(whenList.get(i), false))
                exprList.add(unknownLiteral);
            else
                exprList.add(cx.convertExpression(whenList.get(i)));

            if (SqlUtil.isNullLiteral(thenList.get(i), false))
                exprList.add(nullLiteral);
            else
                exprList.add(cx.convertExpression(thenList.get(i)));
        }

        SqlNode elseOperand = call.getElseOperand();

        if (SqlUtil.isNullLiteral(elseOperand, false))
            exprList.add(nullLiteral);
        else
            exprList.add(cx.convertExpression(requireNonNull(elseOperand, "elseOperand")));

        RelDataType type = rexBuilder.deriveReturnType(call.getOperator(), exprList);

        if (type.getSqlTypeName() == SqlTypeName.CHAR)
            type = TypeUtils.charTypeToVarying(typeFactory, type);

        for (int i : elseArgs(exprList.size()))
            exprList.set(i, rexBuilder.ensureType(type, exprList.get(i), false));

        return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprList);
    }

    /**
     * ELSE operands for CASE operator.
     */
    private static List<Integer> elseArgs(int cnt) {
        // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
        // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
        final List<Integer> list = new ArrayList<>();
        for (int i = cnt % 2;;) {
            list.add(i);
            i += 2;
            if (i >= cnt) {
                list.add(i - 1);
                break;
            }
        }
        return list;
    }

    /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
    private static class TimestampDiffConvertlet implements SqlRexConvertlet {
        /** {@inheritDoc} */
        @Override public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlLiteral unitLiteral = call.operand(0);
            TimeUnit unit = unitLiteral.getValueAs(TimeUnit.class);
            BigDecimal multiplier = BigDecimal.ONE;
            BigDecimal divider = BigDecimal.ONE;
            SqlTypeName sqlTypeName = unit == TimeUnit.NANOSECOND
                ? SqlTypeName.BIGINT
                : SqlTypeName.INTEGER;
            switch (unit) {
                case MICROSECOND:
                case MILLISECOND:
                case NANOSECOND:
                    divider = unit.multiplier;
                    unit = TimeUnit.MILLISECOND;
                    break;
                case WEEK:
                    multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
                    divider = unit.multiplier;
                    unit = TimeUnit.SECOND;
                    break;
                case QUARTER:
                    divider = unit.multiplier;
                    unit = TimeUnit.MONTH;
                    break;
                default:
                    break;
            }
            final SqlIntervalQualifier qualifier =
                new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
            final RexNode op2 = cx.convertExpression(call.operand(2));
            final RexNode op1 = cx.convertExpression(call.operand(1));
            final RelDataType intervalType =
                cx.getTypeFactory().createTypeWithNullability(
                    cx.getTypeFactory().createSqlIntervalType(qualifier),
                    op1.getType().isNullable() || op2.getType().isNullable());
            final RexCall rexCall = (RexCall) rexBuilder.makeCall(
                intervalType, SqlStdOperatorTable.MINUS_DATE,
                ImmutableList.of(op2, op1));
            final RelDataType intType =
                cx.getTypeFactory().createTypeWithNullability(
                    cx.getTypeFactory().createSqlType(sqlTypeName),
                    SqlTypeUtil.containsNullable(rexCall.getType()));

            RexNode e;

            // Since Calcite converts internal time representation to seconds during cast we need our own cast
            // method to keep fraction of seconds.
            if (unit == TimeUnit.MILLISECOND)
                e = makeCastMilliseconds(rexBuilder, intType, rexCall);
            else
                e = rexBuilder.makeCast(intType, rexCall);

            return rexBuilder.multiplyDivide(e, multiplier, divider);
        }

        /**
         * Creates a call to cast milliseconds interval.
         */
        static RexNode makeCastMilliseconds(RexBuilder builder, RelDataType type, RexNode exp) {
            return builder.ensureType(type, builder.decodeIntervalOrDecimal(exp), false);
        }
    }
}
