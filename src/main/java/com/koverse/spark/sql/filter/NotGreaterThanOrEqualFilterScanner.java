package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;

final class NotGreaterThanOrEqualFilterScanner extends AbstractCastingRewriteFilterScanner<GreaterThanOrEqual> {

  public NotGreaterThanOrEqualFilterScanner() {
    super(GreaterThanOrEqual.class);
  }

  @Override
  protected Filter rewrite(GreaterThanOrEqual f) {
    return new LessThan(f.attribute(), f.value());
  }
  
}
