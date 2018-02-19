package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Not;

final class NotNotFilterScanner extends AbstractCastingRewriteFilterScanner<Not> {

  public NotNotFilterScanner() {
    super(Not.class);
  }

  @Override
  protected Filter rewrite(Not f) {
    return f.child();
  }
  
}
