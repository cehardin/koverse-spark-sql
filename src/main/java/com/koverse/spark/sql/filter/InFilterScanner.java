package com.koverse.spark.sql.filter;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Or;

import java.util.Arrays;
import java.util.Iterator;

final class InFilterScanner extends AbstractCastingRewriteFilterScanner<In> {

  public InFilterScanner() {
    super(In.class);
  }

  @Override
  protected Filter rewrite(In f) {
    final Iterator<?> values = Arrays.asList(f.values()).iterator();
    Filter result = new EqualTo(f.attribute(), values.next());

    while (values.hasNext()) {
      result = new Or(
              new EqualTo(f.attribute(), values.next()), 
              result);
    }

    return result;
  }
}
