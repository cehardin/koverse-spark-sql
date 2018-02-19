package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.IsNull;

final class IsNullFilterScanner extends AbstractCastingFilterScanner<IsNull> {

  public IsNullFilterScanner() {
    super(IsNull.class);
  }

  @Override
  protected JavaRDD<String> applyTyped(FilterScannerContext c, IsNull f) {
    return c.getSparkContext().emptyRDD();
  }
  
}
