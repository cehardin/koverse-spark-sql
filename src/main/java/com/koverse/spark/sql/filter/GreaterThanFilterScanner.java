package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.GreaterThan;

final class GreaterThanFilterScanner extends AbstractCastingRangeFilterScanner<GreaterThan> {

  public GreaterThanFilterScanner() {
    super(GreaterThan.class);
  }

  @Override
  protected Range createRange(GreaterThan f) {
    return new Range(
                new Key(f.attribute(), toIndexEntry(f.value())),
                null,
                false,
                true,
                false,
                true);
  }
  
  
  
  
}
