/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.koverse.spark.sql.filter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 *
 * @author cehar
 */
public class IndexEntry {
  private static final char FIELD_DELIMETER = '$';
  private static final Splitter FIELD_SPLITTER = Splitter.on(FIELD_DELIMETER)
          .trimResults()
          .omitEmptyStrings()
          .limit(2);
  private static final Joiner FIELD_JOINER = Joiner.on(FIELD_DELIMETER).skipNulls();
  
  public static IndexEntry create(
          String fieldName, 
          String fieldValue) {
    return new IndexEntry(fieldName, fieldValue, "");
  }
  
  public static IndexEntry create(
          String fieldName, 
          String fieldValue, 
          String recordId) {
    return new IndexEntry(fieldName, fieldValue, recordId);
  }
  
  private final String fieldName;
  private final String fieldValue;
  private final String recordId;
  
  public IndexEntry(final Key key) {
    final List<String> fields = Lists.newArrayList(FIELD_SPLITTER.split(key.getRow().toString()));
    
    fieldName = fields.get(0).trim();
    fieldValue = fields.get(1).trim();
    recordId = key.getColumnFamily().toString().trim();
  }
  
  public IndexEntry(String fieldName, String fieldValue, String recordId) {
    this.fieldName = fieldName.trim();
    this.fieldValue = fieldValue.trim();
    this.recordId = recordId.trim();
  }
  
  public Key toKey() {
    return new Key(
            new Text(FIELD_JOINER.join(fieldName, fieldValue)), 
            new Text(recordId));
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldValue() {
    return fieldValue;
  }

  public String getRecordId() {
    return recordId;
  }
  
  
}
