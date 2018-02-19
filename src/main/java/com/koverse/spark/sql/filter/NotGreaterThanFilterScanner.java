package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThanOrEqual;

final class NotGreaterThanFilterScanner extends AbstractCastingRewriteFilterScanner<GreaterThan> {

  public NotGreaterThanFilterScanner() {
    super(GreaterThan.class);
  }

  @Override
  protected Filter rewrite(GreaterThan f) {
    return new LessThanOrEqual(f.attribute(), f.value());
  }
  
}
