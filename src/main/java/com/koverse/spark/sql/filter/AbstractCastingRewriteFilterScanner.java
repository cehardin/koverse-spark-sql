package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.Filter;

abstract class AbstractCastingRewriteFilterScanner<F extends Filter> extends AbstractCastingFilterScanner<F> {

  AbstractCastingRewriteFilterScanner(Class<F> type) {
    super(type);
  }

  @Override
  protected final JavaRDD<String> applyTyped(FilterScannerContext c, F f) {
    return c.getRootFilterScanner().apply(c, rewrite(f));
  }
 
  protected abstract Filter rewrite(F f);
}
