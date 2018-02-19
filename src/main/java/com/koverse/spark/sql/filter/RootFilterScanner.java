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

final class RootFilterScanner implements FilterScanner {
  
  private final ImmutableMap<Class<? extends Filter>, FilterScanner> filterScanners;
  
  RootFilterScanner() {
    filterScanners = ImmutableMap.<Class<? extends Filter>, FilterScanner>builder()
          .put(And.class, new AndFilterScanner())
          .put(Or.class, new OrFilterScanner())
          .put(Not.class, new NotFilterScanner())
          .put(EqualNullSafe.class, new EqualNullSafeFilterScanner())
          .put(EqualTo.class, new EqualToFilterScanner())
          .put(IsNotNull.class, new IsNotNullFilterScanner())
          .put(IsNull.class, new IsNullFilterScanner())
          .put(In.class, new InFilterScanner())
          .put(GreaterThan.class, new GreaterThanFilterScanner())
          .put(GreaterThanOrEqual.class, new GreaterThanOrEqualFilterScanner())
          .put(LessThan.class, new LessThanOrEqualFilterScanner())
          .put(LessThanOrEqual.class, new LessThanOrEqualFilterScanner())
          .put(StringStartsWith.class, new StringStartsWithFilterScanner())
          .put(StringEndsWith.class, new StringEndsWithFilterScanner())
          .put(StringContains.class, new StringContainsFilterScanner())
          .build();
  }
  @Override
  public JavaRDD<String> apply(FilterScannerContext c, Filter f) {

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
