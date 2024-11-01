package net.edmison.HdrHistogram.hive.udaf;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class HdrHistogramEvaluator extends HdrHistogramEvaluatorBase {

    protected void recordValue(HdrHistogramEvaluatorBase.HdrHistogramAggregationBuffer myagg, Object vObject) {
      long longValue = PrimitiveObjectInspectorUtils.getLong(vObject, valueOI);
      if (longValue > 0) {
        myagg.histogram.recordValue(longValue);
      } else {
        if (!warned) {
          GenericUDAFHdrHistogram.LOG.warn("HDR Histogram cannot process zero or negative values; Ignoring value <= 0: " + longValue);
          warned = true;
        }
      }
    }

  }