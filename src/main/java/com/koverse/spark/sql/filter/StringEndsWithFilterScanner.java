package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.StringEndsWith;

final class StringEndsWithFilterScanner extends AbstractCastingStringFilterScanner<StringEndsWith> {

  public StringEndsWithFilterScanner() {
    super(StringEndsWith.class);
  }

  @Override
  protected String extractFieldName(StringEndsWith f) {
    return f.attribute();
  }

  @Override
  protected Function<IndexEntry, Boolean> createFilter(StringEndsWith f) {
    return e -> e.getFieldValue().endsWith(f.value());
  }
}
