package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;

final class NotOrFilterScanner extends AbstractCastingRewriteFilterScanner<Or> {

  public NotOrFilterScanner() {
    super(Or.class);
  }

  @Override
  protected Filter rewrite(Or f) {
    return new And(
            new Not(f.left()),
            new Not(f.right()));
  }
  
}
