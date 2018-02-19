package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.StringEndsWith;

final class NotStringStartsWithFilterScanner extends AbstractCastingStringFilterScanner<StringEndsWith> {

  public NotStringStartsWithFilterScanner() {
    super(StringEndsWith.class);
  }

  @Override
  protected String extractFieldName(StringEndsWith f) {
    return f.attribute();
  }

  @Override
  protected Function<IndexEntry, Boolean> createFilter(StringEndsWith f) {
    return e -> !e.getFieldValue().startsWith(f.value());
  }
}
