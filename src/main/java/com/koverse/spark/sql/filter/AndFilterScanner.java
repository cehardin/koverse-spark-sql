package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.And;

final class AndFilterScanner extends AbstractCastingFilterScanner<And> {

  public AndFilterScanner() {
    super(And.class);
  }

  @Override
  public JavaRDD<String> applyTyped(FilterScannerContext c, And and) {
    final JavaRDD<String> left = c.getRootFilterScanner().apply(c, and.left());
    final JavaRDD<String> right = c.getRootFilterScanner().apply(c, and.right());
    
    return left.intersection(right);
  }
  
  
}
