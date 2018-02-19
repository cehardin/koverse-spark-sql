package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.LessThanOrEqual;

final class LessThanOrEqualFilterScanner extends AbstractCastingRangeFilterScanner<LessThanOrEqual> {

  public LessThanOrEqualFilterScanner() {
    super(LessThanOrEqual.class);
  }

  @Override
  protected Range createRange(LessThanOrEqual f) {
    return new Range(
                null,
                new Key(f.attribute(), toIndexEntry(f.value())),
                true,
                true,
                true,
                false);
  }
  
  
  
  
}
