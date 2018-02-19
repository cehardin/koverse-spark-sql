package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.Or;

final class OrFilterScanner extends AbstractCastingFilterScanner<Or> {

  public OrFilterScanner() {
    super(Or.class);
  }

  @Override
  public JavaRDD<String> applyTyped(FilterScannerContext c, Or or) {
    final JavaRDD<String> left = c.getRootFilterScanner().apply(c, or.left());
    final JavaRDD<String> right = c.getRootFilterScanner().apply(c, or.right());
    
    return left.union(right);
  }
  
  
}
