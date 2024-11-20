package net.edmison.HdrHistogram.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GenericUDAFHdrHistogram is a user-defined aggregate function (UDAF) for Apache Hive
 * that computes a High Dynamic Range (HDR) Histogram.
 * 
 * This UDAF takes two parameters: 
 * <ol>
 * <li> A column of discrete numeric values (bytes, shorts, ints, or longs) to be measured, 
 * OR a string column containing base64 encoded histogram. 
 * <li> An integer precision value that determines the granularity of the histogram.
 * </ol>
 * 
 * The resulting histogram is encoded into an array of structs in the table/CSV format 
 * produced by HDR Histogram tools. This array can then be graphed to produce a chart
 * in the HDR Histogram style. 
 * 
 * Example usage:
 * SELECT hdr_histogram(val, 3) FROM src;
 * 
 * This method does not support floating point numbers, timestamps, decimals, booleans, or dates.
 */

@SuppressWarnings("deprecation")
@Description(name = "hdr_histogram", value = "_FUNC_(x, precision) - Returns a HDR Histogram JSON of a set of numbers", extended = "Example: \n"
    + "SELECT hdr_histogram(val, 3) from src;\n"
    // TODO insert example output representation, could do x/y like
    // HistogramNumeric.
    + "This creates a HDR histogram for the specified set, with the specified result precision")
public class GenericUDAFHdrHistogram extends AbstractGenericUDAFResolver {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFHdrHistogram.class.getName());

  /**
   * Creates and returns an evaluator for discrete numeric values or for base64 encoded histograms.
   * 
   * @param parameters the parameter types supplied to the UDAF when it was called
   * @return the evaluator for the given parameters
   * @throws SemanticException if the parameters are invalid
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "please specify two arguments.");
    }

    // validate the second parameter, which is the histogram precision
    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "Only primitive type arguments are accepted but "
              + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(1,
          "Only an integer argument is accepted as parameter 2, but "
              + parameters[1].getTypeName() + " was passed instead.");
    }

    // validate the first argument which is the column/data to be measured in the
    // histogram.
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "This function cannot process arguments of type "
              + parameters[0].getTypeName() + " .");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
      case BYTE: 
      case SHORT:
      case INT: 
      case LONG:
        return new HdrHistogramEvaluator();
      case STRING:
      case VARCHAR:
      case CHAR:
        return new HdrEncodedHistogramEvaluator();
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DECIMAL:
      case BOOLEAN:
      case DATE:
      default:
        throw new UDFArgumentTypeException(0,
            "Only discrete numeric arguments are accepted but "
                + parameters[0].getTypeName() + " is passed.");
    }
  }

}
