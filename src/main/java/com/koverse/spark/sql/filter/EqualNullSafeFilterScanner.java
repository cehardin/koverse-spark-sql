package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;

final class EqualNullSafeFilterScanner extends AbstractCastingRewriteFilterScanner<EqualNullSafe> {

  public EqualNullSafeFilterScanner() {
    super(EqualNullSafe.class);
  }

  @Override
  protected Filter rewrite(EqualNullSafe f) {
    return new EqualTo(f.attribute(), f.value());
  }
}
