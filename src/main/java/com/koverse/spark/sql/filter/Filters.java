package com.koverse.spark.sql.filter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.Filter;

import java.util.Collection;
import java.util.function.Supplier;

public final class Filters {
  
  private static final RootFilterScanner ROOT_FILTER_SCANNER = new RootFilterScanner();
  
  public static JavaRDD<String> toRdd(
          final JavaSparkContext sparkContext,
          final Supplier<Job> hadoopJobCreator,
          final Collection<Filter> filters) {
    
    try {
      final FilterScannerContext c = new FilterScannerContext(
              sparkContext,
              ROOT_FILTER_SCANNER,
              hadoopJobCreator);
      
      return filters.stream()
              .map(f -> ROOT_FILTER_SCANNER.apply(c, f))
              .reduce((rdd1, rdd2) -> rdd1.union(rdd2))
              .orElse(sparkContext.emptyRDD());
    } catch (RuntimeException e) {
      throw new RuntimeException(
              String.format(
                      "Could not process filters: %s", 
                      filters),
              e);
    }
  }
}
