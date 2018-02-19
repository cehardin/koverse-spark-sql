package com.koverse.spark.sql.filter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.Filter;

import java.io.Serializable;
import java.util.function.BiFunction;

interface FilterScanner extends BiFunction<FilterScannerContext, Filter, JavaRDD<String>>, Serializable {
}
