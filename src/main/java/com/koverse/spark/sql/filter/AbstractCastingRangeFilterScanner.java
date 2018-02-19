package com.koverse.spark.sql.filter;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.Filter;

import java.util.Arrays;

abstract class AbstractCastingRangeFilterScanner<F extends Filter> extends AbstractCastingFilterScanner<F> {

  protected AbstractCastingRangeFilterScanner(Class<F> type) {
    super(type);
  }

  @Override
  protected final JavaRDD<String> applyTyped(FilterScannerContext c, F filter) {
    final Range range = createRange(filter);
    final Job job = c.createJob();

    AccumuloInputFormat.setRanges(job, Arrays.asList(range));

    return c.getSparkContext().newAPIHadoopRDD(
            job.getConfiguration(),
            AccumuloInputFormat.class,
            Key.class,
            Value.class)
            .keys()
            .map(IndexEntry::new)
            .map(IndexEntry::getRecordId);

  }
  
  protected final String toIndexEntry(Object o) {
    throw new UnsupportedOperationException();
  }

  protected abstract Range createRange(F filter);

}
