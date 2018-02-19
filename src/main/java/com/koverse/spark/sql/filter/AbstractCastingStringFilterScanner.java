package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.sources.Filter;

import java.util.Arrays;

abstract class AbstractCastingStringFilterScanner<F extends Filter> extends AbstractCastingFilterScanner<F> {

  protected AbstractCastingStringFilterScanner(Class<F> type) {
    super(type);
  }

  @Override
  protected final JavaRDD<String> applyTyped(FilterScannerContext c, F f) {
    final Job job = c.createJob();
    final IndexEntry indexEntry = IndexEntry.create(
            extractFieldName(f),
            "");
    final Range range = new Range(
            indexEntry.toKey(),
            null,
            true,
            true,
            false,
            true);

    AccumuloInputFormat.setRanges(job, Arrays.asList(range));

    return c.getSparkContext().newAPIHadoopRDD(
            job.getConfiguration(),
            AccumuloInputFormat.class,
            Key.class,
            Value.class)
            .keys()
            .map(IndexEntry::new)
            .filter(createFilter(f))
            .map(IndexEntry::getRecordId);
  }

  protected abstract String extractFieldName(F f);

  protected abstract Function<IndexEntry, Boolean> createFilter(F f);
}
