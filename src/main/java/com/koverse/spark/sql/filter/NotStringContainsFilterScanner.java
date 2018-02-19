package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.StringContains;

final class NotStringContainsFilterScanner extends AbstractCastingStringFilterScanner<StringContains> {

  public NotStringContainsFilterScanner() {
    super(StringContains.class);
  }

  @Override
  protected String extractFieldName(StringContains f) {
    return f.attribute();
  }

  @Override
  protected Function<IndexEntry, Boolean> createFilter(StringContains f) {
    return e -> !e.getFieldValue().contains(f.value());
  }

}
