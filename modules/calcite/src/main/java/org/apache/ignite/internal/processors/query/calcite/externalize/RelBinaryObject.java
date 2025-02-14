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
package org.apache.ignite.internal.processors.query.calcite.externalize;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteCustomType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Utilities for converting {@link RelNode} into Binary Object format.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class RelBinaryObject {
    /** */
    @SuppressWarnings("PublicInnerClass") @FunctionalInterface
    public interface RelFactory extends Function<RelInput, RelNode> {
        /** {@inheritDoc} */
        @Override RelNode apply(RelInput input);
    }

    /** */
    private static final LoadingCache<String, RelFactory> FACTORIES_CACHE = CacheBuilder.newBuilder()
        .build(CacheLoader.from(RelBinaryObject::relFactory));

    /** */
    private static RelFactory relFactory(String typeName) {
        Class<?> clazz = classForName(typeName, false);

        assert RelNode.class.isAssignableFrom(clazz);

        Constructor<RelNode> constructor;

        try {
            constructor = (Constructor<RelNode>)clazz.getConstructor(RelInput.class);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Class does not have required constructor, " + clazz + "(RelInput)");
        }

        BlockBuilder builder = new BlockBuilder();
        ParameterExpression input_ = Expressions.parameter(RelInput.class);
        builder.add(Expressions.new_(constructor, input_));
        MethodDeclaration declaration = Expressions.methodDecl(
            Modifier.PUBLIC, RelNode.class, "apply", F.asList(input_), builder.toBlock());
        return Commons.compile(RelFactory.class, Expressions.toString(F.asList(declaration), "\n", true));
    }

    /** */
    private static Class<?> classForName(String typeName, boolean skipNotFound) {
        try {
            return U.forName(typeName, U.gridClassLoader());
        }
        catch (ClassNotFoundException e) {
            if (!skipNotFound)
                throw new IgniteException("unknown type " + typeName);
        }

        return null;
    }

    /** Query context. */
    private final BaseQueryContext qctx;

    /** Binary objects processor. */
    private final IgniteBinary binary;

    /** */
    RelBinaryObject(@Nullable IgniteBinary binary, BaseQueryContext qctx) {
        this.binary = binary;
        this.qctx = qctx;
    }

    /** */
    Function<RelInput, RelNode> factory(String type) {
        return FACTORIES_CACHE.getUnchecked(type);
    }

    /** */
    Object toBinary(Object val) {
        if (val == null
            || val instanceof Number // TODO check BigDecimal
            || val instanceof String
            || val instanceof Boolean)
            return val;
        else if (val instanceof Enum)
            return toBinary((Enum)val);
        else if (val instanceof RexNode)
            return toBinary((RexNode)val);
        else if (val instanceof RexWindow)
            return toBinary((RexWindow)val);
        else if (val instanceof RexFieldCollation)
            return toBinary((RexFieldCollation)val);
        else if (val instanceof RexWindowBound)
            return toBinary((RexWindowBound)val);
        else if (val instanceof CorrelationId)
            return toBinary((CorrelationId)val);
        else if (val instanceof List) {
            List<Object> list = list();
            for (Object o : (List)val)
                list.add(toBinary(o));
            return list;
        }
        else if (val instanceof ImmutableBitSet) {
            List<Object> list = list();
            for (Integer integer : (ImmutableBitSet)val)
                list.add(toBinary(integer));
            return list;
        }
        else if (val instanceof Set) {
            Set<Object> set = set();
            for (Object o : (Set)val)
                set.add(toBinary(o));
            return set;
        }
        else if (val instanceof DistributionTrait)
            return toBinary((DistributionTrait)val);
        else if (val instanceof AggregateCall)
            return toBinary((AggregateCall)val);
        else if (val instanceof RelCollationImpl)
            return toBinary((RelCollationImpl)val);
        else if (val instanceof RelDataType)
            return toBinary((RelDataType)val);
        else if (val instanceof RelDataTypeField)
            return toBinary((RelDataTypeField)val);
        else if (val instanceof ByteString)
            return toBinary((ByteString)val);
        else if (val instanceof SearchBounds)
            return toBinary((SearchBounds)val);
        else
            throw new UnsupportedOperationException("type not serializable: "
                + val + " (type " + val.getClass().getCanonicalName() + ")");
    }

    /** */
    RelCollation toCollation(List<BinaryObject> boFieldCollations) {
        if (boFieldCollations == null)
            return RelCollations.EMPTY;

        List<RelFieldCollation> fieldCollations = boFieldCollations.stream()
            .map(this::toFieldCollation)
            .collect(Collectors.toList());

        return RelCollations.of(fieldCollations);
    }

    /** */
    IgniteDistribution toDistribution(BinaryObject bo) {
        Type type = toEnum(bo.field("type"));

        switch (type) {
            case SINGLETON:
                return IgniteDistributions.single();
            case ANY:
                return IgniteDistributions.any();
            case BROADCAST_DISTRIBUTED:
                return IgniteDistributions.broadcast();
            case RANDOM_DISTRIBUTED:
                return IgniteDistributions.random();
            case HASH_DISTRIBUTED:
                break;
            default:
                throw new AssertionError("Unexpected distribution type: " + type);
        }

        Number cacheId = bo.field("cacheId");

        if (cacheId != null) {
            return IgniteDistributions.hash(bo.field("keys"),
                DistributionFunction.affinity(cacheId.intValue(), bo.field("identity")));
        }

        return IgniteDistributions.hash(bo.field("keys"), DistributionFunction.hash());
    }

    /** */
    RelDataType toTypeFields(RelDataTypeFactory typeFactory, List<BinaryObject> fields) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (BinaryObject field : fields)
            builder.add(field.field("name"), toType(typeFactory, field.field("type")));

        return builder.build();
    }

    /** */
    RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
        if (o instanceof List) // Workaround for Values.
            return toTypeFields(typeFactory, (List<BinaryObject>)o);

        BinaryObject bo = (BinaryObject)o;
        String clazz = bo.field("class");

        if (clazz != null) {
            RelDataType type = typeFactory.createJavaType(classForName(clazz, false));

            if (Boolean.TRUE == bo.field("nullable"))
                type = typeFactory.createTypeWithNullability(type, true);

            return type;
        }

        List<BinaryObject> fields = bo.field("fields");

        if (fields != null)
            return toTypeFields(typeFactory, fields);
        else {
            SqlTypeName sqlTypeName = toEnum(bo.field("type"));
            Integer precision = bo.field("precision");
            Integer scale = bo.field("scale");
            RelDataType type = null;

            if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
                TimeUnit startUnit = sqlTypeName.getStartUnit();
                TimeUnit endUnit = sqlTypeName.getEndUnit();
                type = typeFactory.createSqlIntervalType(
                    new SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO));
            }
            else if (sqlTypeName == SqlTypeName.ARRAY)
                type = typeFactory.createArrayType(toType(typeFactory, bo.field("elementType")), -1);
            else if (sqlTypeName == SqlTypeName.MAP)
                type = typeFactory.createMapType(
                    toType(typeFactory, bo.field("keyType")),
                    toType(typeFactory, bo.field("valueType"))
                );
            else if (sqlTypeName == SqlTypeName.ANY) {
                String customType = bo.field("customType");

                if (customType != null)
                    type = ((IgniteTypeFactory)typeFactory).createCustomType(classForName(customType, false));
            }
            else if (precision == null)
                type = typeFactory.createSqlType(sqlTypeName);
            else if (scale == null)
                type = typeFactory.createSqlType(sqlTypeName, precision);

            if (type == null)
                type = typeFactory.createSqlType(sqlTypeName, precision, scale);

            if (Boolean.TRUE == bo.field("nullable"))
                type = typeFactory.createTypeWithNullability(type, true);

            return type;
        }
    }

    /** */
    RexNode toRex(RelInput relInput, BinaryObject bo) {
        RelOptCluster cluster = relInput.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        if (bo == null)
            return null;

        BinaryObject boOp = bo.field("op");
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        if (boOp != null) {
            List operands = bo.field("operands");
            List<RexNode> rexOperands = toRexList(relInput, operands);
            BinaryObject boType = bo.field("type");
            BinaryObject boWindow = bo.field("window");
            if (boWindow != null) {
                SqlAggFunction operator = (SqlAggFunction)toOp(boOp);
                RelDataType type = toType(typeFactory, boType);
                List<RexNode> partitionKeys = new ArrayList<>();
                if (boWindow.hasField("partition"))
                    partitionKeys = toRexList(relInput, boWindow.field("partition"));
                List<RexFieldCollation> orderKeys = new ArrayList<>();
                if (boWindow.hasField("order"))
                    orderKeys = toRexFieldCollationList(relInput, boWindow.field("order"));
                RexWindowBound lowerBound;
                RexWindowBound upperBound;
                boolean physical;
                if (boWindow.field("rows-lower") != null) {
                    lowerBound = toRexWindowBound(relInput, boWindow.field("rows-lower"));
                    upperBound = toRexWindowBound(relInput, boWindow.field("rows-upper"));
                    physical = true;
                }
                else if (boWindow.field("range-lower") != null) {
                    lowerBound = toRexWindowBound(relInput, boWindow.field("range-lower"));
                    upperBound = toRexWindowBound(relInput, boWindow.field("range-upper"));
                    physical = false;
                }
                else {
                    // No ROWS or RANGE clause
                    lowerBound = null;
                    upperBound = null;
                    physical = false;
                }
                boolean distinct = bo.field("distinct");
                return rexBuilder.makeOver(type, operator, rexOperands, partitionKeys,
                    ImmutableList.copyOf(orderKeys), lowerBound, upperBound, physical,
                    true, false, distinct, false);
            }
            else {
                SqlOperator operator = toOp(boOp);
                RelDataType type;
                if (boType != null)
                    type = toType(typeFactory, boType);
                else
                    type = rexBuilder.deriveReturnType(operator, rexOperands);
                return rexBuilder.makeCall(type, operator, rexOperands);
            }
        }

        Integer input = bo.field("input");
        if (input != null) {
            // Check if it is a local ref.
            BinaryObject boType = bo.field("type");

            if (boType != null) {
                RelDataType type = toType(typeFactory, boType);
                return bo.field("dynamic") == Boolean.TRUE
                    ? rexBuilder.makeDynamicParam(type, input)
                    : rexBuilder.makeLocalRef(type, input);
            }

            List<RelNode> inputNodes = relInput.getInputs();
            int i = input;
            for (RelNode inputNode : inputNodes) {
                RelDataType rowType = inputNode.getRowType();
                if (i < rowType.getFieldCount()) {
                    RelDataTypeField field = rowType.getFieldList().get(i);
                    return rexBuilder.makeInputRef(field.getType(), input);
                }
                i -= rowType.getFieldCount();
            }
            throw new RuntimeException("input field " + input + " is out of range");
        }

        String field = bo.field("field");
        if (field != null) {
            BinaryObject boExpr = bo.field("expr");
            RexNode expr = toRex(relInput, boExpr);
            return rexBuilder.makeFieldAccess(expr, field, true);
        }

        String correl = bo.field("correl");
        if (correl != null) {
            RelDataType type = toType(typeFactory, bo.field("type"));
            return rexBuilder.makeCorrel(type, new CorrelationId(correl));
        }

        if (bo.hasField("literal")) {
            Object literal = bo.field("literal");
            RelDataType type = toType(typeFactory, bo.field("type"));

            if (literal == null)
                return rexBuilder.makeNullLiteral(type);

            if (type.getSqlTypeName() == SqlTypeName.SYMBOL)
                literal = toEnum(literal);
            else if (type.getSqlTypeName().getFamily() == SqlTypeFamily.BINARY)
                literal = toByteString(literal);
            else if (type.getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC && literal instanceof Number)
                literal = SqlFunctions.toBigDecimal((Number)literal);

            return rexBuilder.makeLiteral(literal, type, true);
        }

        throw new UnsupportedOperationException("cannot convert to rex " + bo);
    }

    /** */
    SqlOperator toOp(BinaryObject bo) {
        // In case different operator has the same kind, check with both name and kind.
        String name = bo.field("name");
        SqlKind sqlKind = toEnum(bo.field("kind"));
        SqlSyntax sqlSyntax = toEnum(bo.field("syntax"));
        List<SqlOperator> operators = new ArrayList<>();

        qctx.opTable().lookupOperatorOverloads(
            new SqlIdentifier(name, new SqlParserPos(0, 0)),
            null,
            sqlSyntax,
            operators,
            SqlNameMatchers.liberal()
        );

        for (SqlOperator operator : operators) {
            if (operator.kind == sqlKind)
                return operator;
        }

        String cls_ = bo.field("class");

        if (cls_ != null)
            return AvaticaUtils.instantiatePlugin(SqlOperator.class, cls_);

        return null;
    }

    /** */
    <T> List<T> list() {
        return new ArrayList<>();
    }

    /** */
    <T> Set<T> set() {
        return new LinkedHashSet<>();
    }

    /** */
    BinaryObjectBuilder builder(Object obj) {
        return binary.builder(obj.getClass().getName());
    }

    /** */
    <T extends Enum<T>> T toEnum(Object o) {
        return ((BinaryObject)o).deserialize();
    }

    /** */
    private ByteString toByteString(Object o) {
        assert o instanceof String;

        return ByteString.of((String)o, 16);
    }

    /** */
    private RelFieldCollation toFieldCollation(BinaryObject bo) {
        Integer field = bo.field("field");
        Direction direction = toEnum(bo.field("direction"));
        NullDirection nullDirection = toEnum(bo.field("nulls"));

        return new RelFieldCollation(field, direction, nullDirection);
    }

    /** */
    private List<RexFieldCollation> toRexFieldCollationList(RelInput relInput, List<BinaryObject> order) {
        if (order == null)
            return null;

        List<RexFieldCollation> list = new ArrayList<>();
        for (BinaryObject bo : order) {
            RexNode expr = toRex(relInput, bo.field("expr"));
            Set<SqlKind> directions = new HashSet<>();
            if (toEnum(bo.field("direction")) == Direction.DESCENDING)
                directions.add(SqlKind.DESCENDING);
            if (toEnum(bo.field("null-direction")) == NullDirection.FIRST)
                directions.add(SqlKind.NULLS_FIRST);
            else
                directions.add(SqlKind.NULLS_LAST);
            list.add(new RexFieldCollation(expr, directions));
        }
        return list;
    }

    /** */
    private RexWindowBound toRexWindowBound(RelInput input, BinaryObject bo) {
        if (bo == null)
            return null;

        String type = bo.field("type");
        switch (type) {
            case "CURRENT_ROW":
                return RexWindowBounds.create(
                    SqlWindow.createCurrentRow(SqlParserPos.ZERO), null);
            case "UNBOUNDED_PRECEDING":
                return RexWindowBounds.create(
                    SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
            case "UNBOUNDED_FOLLOWING":
                return RexWindowBounds.create(
                    SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null);
            case "PRECEDING":
                RexNode precedingOffset = toRex(input, bo.field("offset"));
                return RexWindowBounds.create(null,
                    input.getCluster().getRexBuilder().makeCall(
                        SqlWindow.PRECEDING_OPERATOR, precedingOffset));
            case "FOLLOWING":
                RexNode followingOffset = toRex(input, bo.field("offset"));
                return RexWindowBounds.create(null,
                    input.getCluster().getRexBuilder().makeCall(
                        SqlWindow.FOLLOWING_OPERATOR, followingOffset));
            default:
                throw new UnsupportedOperationException("cannot convert type to rex window bound " + type);
        }
    }

    /** */
    private List<RexNode> toRexList(RelInput relInput, List<BinaryObject> operands) {
        List<RexNode> list = new ArrayList<>(operands.size());

        for (BinaryObject operand : operands)
            list.add(toRex(relInput, operand));

        return list;
    }

    /** */
    private SearchBounds toSearchBound(RelInput input, BinaryObject bo) {
        if (bo == null)
            return null;

        String type = bo.field("type");

        if (SearchBounds.Type.EXACT.name().equals(type))
            return new ExactBounds(null, toRex(input, bo.field("bound")));
        else if (SearchBounds.Type.MULTI.name().equals(type))
            return new MultiBounds(null, toSearchBoundList(input, bo.field("bounds")));
        else if (SearchBounds.Type.RANGE.name().equals(type)) {
            return new RangeBounds(null,
                toRex(input, bo.field("lowerBound")),
                toRex(input, bo.field("upperBound")),
                bo.field("lowerInclude"),
                bo.field("upperInclude")
            );
        }

        throw new IllegalStateException("Unsupported search bound type: " + type);
    }

    /** */
    List<SearchBounds> toSearchBoundList(RelInput input, List<BinaryObject> bounds) {
        if (bounds == null)
            return null;

        return bounds.stream().map(b -> toSearchBound(input, b)).collect(Collectors.toList());
    }

    /** */
    private Object toBinary(Enum<?> enum0) {
        return enum0;
    }

    /** */
    private Object toBinary(AggregateCall node) {
        BinaryObjectBuilder builder = builder(node);

        builder.setField("agg", toBinary(node.getAggregation()));
        builder.setField("type", toBinary(node.getType()));
        builder.setField("distinct", node.isDistinct());
        builder.setField("operands", node.getArgList());
        builder.setField("filter", node.filterArg);
        builder.setField("name", node.getName());
        builder.setField("coll", toBinary(node.getCollation()));
        builder.setField("rexList", toBinary(node.rexList));

        return builder.build();
    }

    /** */
    private Object toBinary(RelDataType node) {
        if (node instanceof JavaType) {
            BinaryObjectBuilder builder = builder(node);

            builder.setField("class", ((JavaType)node).getJavaClass().getName());
            builder.setField("nullable", node.isNullable());

            return builder.build();
        }

        BinaryObjectBuilder builder = builder(node);

        builder.setField("type", toBinary(node.getSqlTypeName()));

        if (node.getSqlTypeName() == SqlTypeName.ARRAY) {
            builder.setField("elementType", toBinary(node.getComponentType()));

            return builder.build();
        }
        else if (node.getSqlTypeName() == SqlTypeName.MAP) {
            builder.setField("keyType", toBinary(node.getKeyType()));
            builder.setField("valueType", toBinary(node.getValueType()));

            return builder.build();
        }
        else {
            if (node.getSqlTypeName() == SqlTypeName.ANY && node instanceof IgniteCustomType)
                builder.setField("customType", ((IgniteCustomType)node).storageType().getTypeName());

            builder.setField("nullable", node.isNullable());

            if (node.getSqlTypeName().allowsPrec())
                builder.setField("precision", node.getPrecision());

            if (node.getSqlTypeName().allowsScale())
                builder.setField("scale", node.getScale());

            if (node.isStruct()) {
                List<Object> list = list();

                for (RelDataTypeField field : node.getFieldList())
                    list.add(toBinary(field));

                builder.setField("fields", list);
            }

            return builder.build();
        }
    }

    /** */
    private Object toBinary(RelDataTypeField node) {
        BinaryObjectBuilder builder = builder(node);

        builder.setField("name", node.getName());
        builder.setField("type", toBinary(node.getType()));

        return builder.build();
    }

    /** */
    private Object toBinary(CorrelationId node) {
        return node.getId();
    }

    /** */
    private Object toBinary(RexNode node) {
        // removes calls to SEARCH and the included Sarg and converts them to comparisons
        node = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, node);

        BinaryObjectBuilder builder = builder(node);

        switch (node.getKind()) {
            case FIELD_ACCESS:
                RexFieldAccess fieldAccess = (RexFieldAccess)node;
                builder.setField("field", fieldAccess.getField().getName());
                builder.setField("expr", toBinary(fieldAccess.getReferenceExpr()));

                return builder.build();
            case LITERAL:
                RexLiteral literal = (RexLiteral)node;
                Object val = literal.getValue3();

                builder.setField("literal", toBinary(val));
                builder.setField("type", toBinary(node.getType()));

                return builder.build();
            case INPUT_REF:
                builder.setField("input", ((RexSlot)node).getIndex());
                builder.setField("name", ((RexVariable)node).getName());

                return builder.build();
            case DYNAMIC_PARAM:
                builder.setField("input", ((RexDynamicParam)node).getIndex());
                builder.setField("name", ((RexVariable)node).getName());
                builder.setField("type", toBinary(node.getType()));
                builder.setField("dynamic", true);

                return builder.build();
            case LOCAL_REF:
                builder.setField("input", ((RexSlot)node).getIndex());
                builder.setField("name", ((RexVariable)node).getName());
                builder.setField("type", toBinary(node.getType()));

                return builder.build();
            case CORREL_VARIABLE:
                builder.setField("correl", ((RexVariable)node).getName());
                builder.setField("type", toBinary(node.getType()));

                return builder.build();
            default:
                if (node instanceof RexCall) {
                    RexCall call = (RexCall)node;

                    builder.setField("op", toBinary(call.getOperator()));
                    List<Object> list = list();

                    for (RexNode operand : call.getOperands())
                        list.add(toBinary(operand));

                    builder.setField("operands", list);
                    builder.setField("type", toBinary(node.getType()));

                    if (call.getOperator() instanceof SqlFunction) {
                        if (((SqlFunction)call.getOperator()).getFunctionType().isUserDefined()) {
                            SqlOperator op = call.getOperator();
                            builder.setField("class", op.getClass().getName());
                            builder.setField("deterministic", op.isDeterministic());
                            builder.setField("dynamic", op.isDynamicFunction());
                        }
                    }

                    if (call instanceof RexOver) {
                        RexOver over = (RexOver)call;
                        builder.setField("distinct", over.isDistinct());
                        builder.setField("window", toBinary(over.getWindow()));
                    }

                    return builder.build();
                }
                throw new UnsupportedOperationException("unknown rex " + node);
        }
    }

    /** */
    private Object toBinary(RexWindow window) {
        BinaryObjectBuilder builder = builder(window);

        if (!window.partitionKeys.isEmpty())
            builder.setField("partition", toBinary(window.partitionKeys));
        if (!window.orderKeys.isEmpty())
            builder.setField("order", toBinary(window.orderKeys));
        if (window.getLowerBound() == null) {
            // No ROWS or RANGE clause
        }
        else if (window.getUpperBound() == null)
            if (window.isRows())
                builder.setField("rows-lower", toBinary(window.getLowerBound()));
            else
                builder.setField("range-lower", toBinary(window.getLowerBound()));
        else if (window.isRows()) {
            builder.setField("rows-lower", toBinary(window.getLowerBound()));
            builder.setField("rows-upper", toBinary(window.getUpperBound()));
        }
        else {
            builder.setField("range-lower", toBinary(window.getLowerBound()));
            builder.setField("range-upper", toBinary(window.getUpperBound()));
        }

        return builder.build();
    }

    /** */
    private Object toBinary(DistributionTrait distribution) {
        Type type = distribution.getType();

        BinaryObjectBuilder builder = builder(distribution);
        builder.setField("type", toBinary(type));

        switch (type) {
            case ANY:
            case BROADCAST_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
            case SINGLETON:
                return builder.build();
            case HASH_DISTRIBUTED:
                List<Object> keys = list();
                for (Integer key : distribution.getKeys())
                    keys.add(toBinary(key));

                builder.setField("keys", keys);

                DistributionFunction function = distribution.function();

                if (function.affinity()) {
                    builder.setField("cacheId", function.cacheId());
                    builder.setField("identity", function.identity().toString());
                }

                return builder.build();
            default:
                throw new AssertionError("Unexpected distribution type.");
        }
    }

    /** */
    private Object toBinary(RelCollationImpl node) {
        List<Object> list = list();

        for (RelFieldCollation fieldCollation : node.getFieldCollations()) {
            BinaryObjectBuilder builder = builder(fieldCollation);

            builder.setField("field", fieldCollation.getFieldIndex());
            builder.setField("direction", toBinary(fieldCollation.getDirection()));
            builder.setField("nulls", toBinary(fieldCollation.nullDirection));

            list.add(builder);
        }

        return list;
    }

    /** */
    private Object toBinary(RexFieldCollation collation) {
        BinaryObjectBuilder builder = builder(collation);

        builder.setField("expr", toBinary(collation.left));
        builder.setField("direction", toBinary(collation.getDirection()));
        builder.setField("null-direction", toBinary(collation.getNullDirection()));

        return builder.build();
    }

    /** */
    private Object toBinary(RexWindowBound windowBound) {
        BinaryObjectBuilder builder = builder(windowBound);

        if (windowBound.isCurrentRow())
            builder.setField("type", "CURRENT_ROW");
        else if (windowBound.isUnbounded())
            builder.setField("type", windowBound.isPreceding() ? "UNBOUNDED_PRECEDING" : "UNBOUNDED_FOLLOWING");
        else {
            builder.setField("type", windowBound.isPreceding() ? "PRECEDING" : "FOLLOWING");
            builder.setField("offset", toBinary(windowBound.getOffset()));
        }
        return builder.build();
    }

    /** */
    private Object toBinary(SqlOperator operator) {
        // User-defined operators are not yet handled.
        BinaryObjectBuilder builder = builder(operator);

        builder.setField("name", operator.getName());
        builder.setField("kind", toBinary(operator.kind));
        builder.setField("syntax", toBinary(operator.getSyntax()));

        return builder.build();
    }

    /** */
    private Object toBinary(ByteString val) {
        return val.toString();
    }

    /** */
    private Object toBinary(SearchBounds val) {
        BinaryObjectBuilder builder = builder(val);

        builder.setField("type", val.type().name());

        if (val instanceof ExactBounds)
            builder.setField("bound", toBinary(((ExactBounds)val).bound()));
        else if (val instanceof MultiBounds)
            builder.setField("bounds", toBinary(((MultiBounds)val).bounds()));
        else {
            assert val instanceof RangeBounds : val;

            RangeBounds val0 = (RangeBounds)val;

            builder.setField("lowerBound", val0.lowerBound() == null ? null : toBinary(val0.lowerBound()));
            builder.setField("upperBound", val0.upperBound() == null ? null : toBinary(val0.upperBound()));
            builder.setField("lowerInclude", val0.lowerInclude());
            builder.setField("upperInclude", val0.upperInclude());
        }

        return builder.build();
    }
}
