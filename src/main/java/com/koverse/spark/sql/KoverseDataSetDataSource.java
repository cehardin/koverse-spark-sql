/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.koverse.spark.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.BucketSpec;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Map;

/**
 *
 * @author cehar
 */
public class KoverseDataSetDataSource extends DataSource {
  
  public KoverseDataSetDataSource(SparkSession ss, String string, Seq<String> seq, Option<StructType> option, Seq<String> seq1, Option<BucketSpec> option1, Map<String, String> map, Option<CatalogTable> option2) {
    super(ss, string, seq, option, seq1, option1, map, option2);
  }
  
  
}
