package com.koverse.spark.sql;

import com.koverse.spark.sql.filter.Filters;

import com.google.common.base.Charsets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author cehar
 */
public class KoverseDataSetRelation extends BaseRelation implements TableScan, PrunedScan, PrunedFilteredScan {

  private final SQLContext sqlContext;
  private final JavaSparkContext sparkContext;
  private final Configuration hadoopConfiguration;
  private final ClientContext accumuloClientContext;
  private final Authorizations accumuloAuthorizations;
  private final String accumuloIndexTableName;
  private final String accumuloRecordTableName;

  public KoverseDataSetRelation(
          SQLContext sqlContext,
          Configuration hadoopConfiguration,
          ClientContext accumuloClientContext,
          Authorizations accumuloAuthorizations,
          String accumuloIndexTableName,
          String accumuloRecordTableName) {

    this.sqlContext = sqlContext;
    this.sparkContext = new JavaSparkContext(sqlContext.sparkContext());
    this.hadoopConfiguration = hadoopConfiguration;
    this.accumuloClientContext = accumuloClientContext;
    this.accumuloAuthorizations = accumuloAuthorizations;
    this.accumuloIndexTableName = accumuloIndexTableName;
    this.accumuloRecordTableName = accumuloRecordTableName;
  }

  private Job createJob(String tableName) {
    final Job job;

    try {
      job = new Job(new Configuration(hadoopConfiguration));
    } catch (IOException e) {
      throw new RuntimeException("Unable to create Hadoop configuration", e);
    }

    try {
      AccumuloInputFormat.setConnectorInfo(
              job,
              accumuloClientContext.getCredentials().getPrincipal(),
              accumuloClientContext.getCredentials().getToken());
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException("Unable to set Accumulo connector information", e);
    }

    AccumuloInputFormat.setScanAuthorizations(job, accumuloAuthorizations);
    AccumuloInputFormat.setZooKeeperInstance(
            job,
            accumuloClientContext.getInstance().getInstanceName(),
            accumuloClientContext.getInstance().getZooKeepers());
    AccumuloInputFormat.setInputTableName(job, tableName);

    return job;
  }

  @Override
  public RDD<Row> buildScan() {
    return buildScan(schema().fieldNames());
  }

  @Override
  public RDD<Row> buildScan(String[] fields) {
    final Job job = createJob(accumuloRecordTableName);
    final Set<String> fieldSet = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(fields)));

    return sparkContext.newAPIHadoopRDD(
            job.getConfiguration(),
            AccumuloInputFormat.class,
            Key.class,
            Value.class)
            .map(t -> new SimpleEntry<>(t._1(), t._2()))
            .map(t -> convertToRow(t, fieldSet))
            .rdd();

  }

  @Override
  public RDD<Row> buildScan(String[] fields, Filter[] filters) {
    final Set<String> fieldSet = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(fields)));
    final String instanceName = accumuloClientContext.getInstance().getInstanceName();
    final String zookeepers = accumuloClientContext.getInstance().getZooKeepers();
    final String credentialsString = accumuloClientContext.getCredentials().serialize();
    final String tableName = accumuloRecordTableName;
    final byte[] authorizationsBytes = accumuloAuthorizations.serialize().getBytes(Charsets.UTF_8);
    final JavaRDD<String> recordIds = Filters.toRdd(
            sparkContext,
            () -> createJob(accumuloIndexTableName),
            Arrays.asList(filters));

    return recordIds
            .distinct()
            .sortBy(id -> id, true, recordIds.getNumPartitions())
            .mapPartitions(ids -> {
              try {
                final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
                final Credentials credentials = Credentials.deserialize(credentialsString);
                final Connector connector = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
                final Authorizations authorizations = new Authorizations(authorizationsBytes);
                final List<Range> ranges = new ArrayList<>();

                ids.forEachRemaining(id -> ranges.add(Range.exact(id)));

                try (final BatchScanner scanner = connector.createBatchScanner(tableName, authorizations, 8)) {
                  final List<Row> rows = new ArrayList<>();

                  scanner.setRanges(Range.mergeOverlapping(ranges));

                  scanner.iterator().forEachRemaining(entry -> rows.add(convertToRow(entry, fieldSet)));

                  return rows.iterator();
                }
              } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                throw new RuntimeException("Unable to load records", e);
              }
            })
            .rdd();
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public StructType schema() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private Row convertToRow(Entry<Key, Value> tuple, Set<String> fields) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
