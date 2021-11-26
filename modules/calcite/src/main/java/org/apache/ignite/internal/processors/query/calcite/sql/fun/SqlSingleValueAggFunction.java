package org.apache.ignite.internal.processors.query.calcite.sql.fun;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlSingleValueAggFunction extends SqlAggFunction {
  //~ Instance fields --------------------------------------------------------

  @Deprecated // to be removed before 2.0
  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  public SqlSingleValueAggFunction(
      RelDataType type) {
    super(
        "SINGLE_VALUE",
        null,
        SqlKind.SINGLE_VALUE,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM,
        false,
        false,
        Optionality.FORBIDDEN);
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean allowsFilter() {
    return false;
  }

  @SuppressWarnings("deprecation")
  @Override public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  @SuppressWarnings("deprecation")
  @Override public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }

  @Deprecated // to be removed before 2.0
  public RelDataType getType() {
    return type;
  }

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }

  @Override public Optionality getDistinctOptionality() {
    return Optionality.IGNORED;
  }

  @Override public SqlAggFunction getRollup() {
    return this;
  }
}
