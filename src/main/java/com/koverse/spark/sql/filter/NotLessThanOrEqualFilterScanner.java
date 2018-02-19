package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThanOrEqual;

final class NotLessThanOrEqualFilterScanner extends AbstractCastingRewriteFilterScanner<LessThanOrEqual> {

  public NotLessThanOrEqualFilterScanner() {
    super(LessThanOrEqual.class);
  }

  @Override
  protected Filter rewrite(LessThanOrEqual f) {
    return new GreaterThan(f.attribute(), f.value());
  }
  
}
