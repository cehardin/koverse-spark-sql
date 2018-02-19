package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.IsNotNull;

final class IsNotNullFilterScanner extends AbstractCastingRangeFilterScanner<IsNotNull> {

  public IsNotNullFilterScanner() {
    super(IsNotNull.class);
  }

  @Override
  protected Range createRange(IsNotNull f) {
    return new Range();
  }
  
  
  
  
}
