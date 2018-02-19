package com.koverse.spark.sql.filter;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;

import java.util.Objects;

final class NotFilterScanner extends AbstractCastingFilterScanner<Not> {

  private final ImmutableMap<Class<? extends Filter>, FilterScanner> filterScanners;

  public NotFilterScanner() {
    super(Not.class);
    
    
  filterScanners = ImmutableMap.<Class<? extends Filter>, FilterScanner>builder()
          .put(And.class, new NotAndFilterScanner())
          .put(Or.class, new NotOrFilterScanner())
          .put(Not.class, new NotNotFilterScanner())
          .put(EqualNullSafe.class, new NotEqualNullSafeFilterScanner())
          .put(EqualTo.class, new NotEqualToFilterScanner())
          .put(GreaterThan.class, new NotGreaterThanFilterScanner())
          .put(GreaterThanOrEqual.class, new NotGreaterThanOrEqualFilterScanner())
          .put(IsNotNull.class, new NotIsNotNullFilterScanner())
          .put(IsNull.class, new NotIsNullFilterScanner())
          .put(In.class, new NotInFilterScanner())
          .put(LessThan.class, new NotLessThanFilterScanner())
          .put(LessThanOrEqual.class, new NotLessThanOrEqualFilterScanner())
          .put(StringContains.class, new NotStringContainsFilterScanner())
          .put(StringStartsWith.class, new NotStringStartsWithFilterScanner())
          .put(StringEndsWith.class, new NotStringEndsWithFilterScanner())
          .build();
  }

  @Override
  protected JavaRDD<String> applyTyped(FilterScannerContext c, Not f) {
    try {
      return Objects.requireNonNull(
              filterScanners.get(f.getClass()),
              () -> String.format("No scanner for filter: %s", f))
              .apply(c, f);
    } catch (RuntimeException e) {
      throw new RuntimeException(
              String.format("Unable to process filter: %s", f),
              e);
    }
  }

}
