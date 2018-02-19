package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;

final class NotLessThanFilterScanner extends AbstractCastingRewriteFilterScanner<LessThan> {

  public NotLessThanFilterScanner() {
    super(LessThan.class);
  }

  @Override
  protected Filter rewrite(LessThan f) {
    return new GreaterThanOrEqual(f.attribute(), f.value());
  }
  
}
