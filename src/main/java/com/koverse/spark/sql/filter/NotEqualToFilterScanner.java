package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Or;

final class NotEqualToFilterScanner extends AbstractCastingRewriteFilterScanner<EqualTo> {

  public NotEqualToFilterScanner() {
    super(EqualTo.class);
  }

  @Override
  protected Filter rewrite(EqualTo f) {
    return new Or(
            new LessThan(f.attribute(), f.value()),
            new GreaterThan(f.attribute(), f.value()));
  }
}
