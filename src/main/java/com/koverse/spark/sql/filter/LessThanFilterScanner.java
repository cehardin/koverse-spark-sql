package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.LessThan;

final class LessThanFilterScanner extends AbstractCastingRangeFilterScanner<LessThan> {

  public LessThanFilterScanner() {
    super(LessThan.class);
  }

  @Override
  protected Range createRange(LessThan f) {
    return new Range(
                null,
                new Key(f.attribute(), toIndexEntry(f.value())),
                true,
                false,
                true,
                false);
  }
  
  
  
  
}
