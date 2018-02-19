package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.Filter;

abstract class AbstractCastingFilterScanner<F extends Filter> implements FilterScanner {

  private final Class<F> type;

  public AbstractCastingFilterScanner(Class<F> type) {
    this.type = type;
  }

  @Override
  public final JavaRDD<String> apply(FilterScannerContext c, Filter f) {
    try {
      return applyTyped(c, type.cast(f));
    } catch (RuntimeException e) {
      throw new RuntimeException(
              String.format("Unable to process filter: %s", f),
              e);
    }

  }

  protected abstract JavaRDD<String> applyTyped(FilterScannerContext c, F f);

}
