package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;

final class NotAndFilterScanner extends AbstractCastingRewriteFilterScanner<And> {

  public NotAndFilterScanner() {
    super(And.class);
  }

  @Override
  protected Filter rewrite(And f) {
    return new Or(
            new Not(f.left()),
            new Not(f.right()));
  }
  
}
