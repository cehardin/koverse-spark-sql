package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.StringEndsWith;

final class NotStringEndsWithFilterScanner extends AbstractCastingStringFilterScanner<StringEndsWith> {

  public NotStringEndsWithFilterScanner() {
    super(StringEndsWith.class);
  }

  @Override
  protected String extractFieldName(StringEndsWith f) {
    return f.attribute();
  }

  @Override
  protected Function<IndexEntry, Boolean> createFilter(StringEndsWith f) {
    return e -> !e.getFieldValue().endsWith(f.value());
  }
  
}
