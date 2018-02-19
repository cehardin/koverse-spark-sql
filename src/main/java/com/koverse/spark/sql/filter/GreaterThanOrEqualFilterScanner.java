package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.GreaterThanOrEqual;

final class GreaterThanOrEqualFilterScanner extends AbstractCastingRangeFilterScanner<GreaterThanOrEqual> {

  public GreaterThanOrEqualFilterScanner() {
    super(GreaterThanOrEqual.class);
  }

  @Override
  protected Range createRange(GreaterThanOrEqual f) {
    return new Range(
                new Key(f.attribute(), toIndexEntry(f.value())),
                null,
                true,
                true,
                false,
                true);
  }
  
  
  
  
}
