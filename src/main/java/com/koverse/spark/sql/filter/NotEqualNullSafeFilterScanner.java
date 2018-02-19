package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Not;

final class NotEqualNullSafeFilterScanner extends AbstractCastingRewriteFilterScanner<EqualNullSafe> {

  public NotEqualNullSafeFilterScanner() {
    super(EqualNullSafe.class);
  }

  @Override
  protected Filter rewrite(EqualNullSafe f) {
    return new Not(new EqualTo(f.attribute(), f.value()));
  }
}
