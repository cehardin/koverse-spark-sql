package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.StringStartsWith;

final class StringStartsWithFilterScanner extends AbstractCastingRewriteFilterScanner<StringStartsWith> {

  public StringStartsWithFilterScanner() {
    super(StringStartsWith.class);
  }

  @Override
  protected Filter rewrite(StringStartsWith f) {
    return new GreaterThanOrEqual(f.attribute(), f.value());
  }
}
