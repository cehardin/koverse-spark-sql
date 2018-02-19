package com.koverse.spark.sql.filter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.function.Supplier;

final class FilterScannerContext implements Serializable {
  private final JavaSparkContext sparkContext;
  private final RootFilterScanner rootFilterScanner;
  private final Supplier<Job> jobCreator;
  
  public FilterScannerContext(
          JavaSparkContext sparkContext,
          RootFilterScanner rootFilterScanner,
          Supplier<Job> jobCreator) {
    this.sparkContext = sparkContext;
    this.rootFilterScanner = rootFilterScanner;
    this.jobCreator = jobCreator;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }
  
  public RootFilterScanner getRootFilterScanner() {
    return rootFilterScanner;
  }
  
  public Job createJob() {
    return jobCreator.get();
  }
  
  
}
