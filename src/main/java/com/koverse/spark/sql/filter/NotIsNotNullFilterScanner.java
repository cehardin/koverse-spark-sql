package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;

final class NotIsNotNullFilterScanner extends AbstractCastingRewriteFilterScanner<IsNotNull> {

  public NotIsNotNullFilterScanner() {
    super(IsNotNull.class);
  }

  @Override
  protected Filter rewrite(IsNotNull f) {
    return new IsNull(f.attribute());
  }
  
}
