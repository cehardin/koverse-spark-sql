package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.spark.sql.sources.EqualTo;

final class EqualToFilterScanner extends AbstractCastingRangeFilterScanner<EqualTo> {

  public EqualToFilterScanner() {
    super(EqualTo.class);
  }

  @Override
  protected Range createRange(EqualTo f) {
    return new Range(
                IndexEntry.create(f.attribute(), toIndexEntry(f.value())).toKey(),
                IndexEntry.create(f.attribute(), toIndexEntry(f.value())).toKey(),
                true,
                true,
                false,
                false);
  }
  
  
  
  
}
