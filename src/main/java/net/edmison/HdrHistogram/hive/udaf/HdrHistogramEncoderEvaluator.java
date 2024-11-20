package net.edmison.HdrHistogram.hive.udaf;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * HdrHistogramEncoderEvaluator is a user-defined aggregation function (UDAF) for Apache Hive
 * that aggregates HDR Histograms.
 *
 * This class is responsible for recording long integer values and aggregating them 
 * into a histogram. HDR Histograms are intended for positive values, so 
 * negative or zero values are not recorded, and a warning is issued if any such 
 * values are encountered.
 * 
 * This class leverages the getResult method of HdrHistogramEncoderEvaluatorBase to 
 * return the aggregated HDR Histogram as a base64-encoded string.
 */ 

public class HdrHistogramEncoderEvaluator extends HdrHistogramEncoderEvaluatorBase {

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