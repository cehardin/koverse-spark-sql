package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;

final class NotIsNullFilterScanner extends AbstractCastingRewriteFilterScanner<IsNull> {

  public NotIsNullFilterScanner() {
    super(IsNull.class);
  }

  @Override
  protected Filter rewrite(IsNull f) {
    return new IsNotNull(f.attribute());
  }
 
}
