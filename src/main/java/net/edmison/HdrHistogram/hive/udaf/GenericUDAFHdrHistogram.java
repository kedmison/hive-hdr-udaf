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
 * be inspired by GenericUDAFHistogramNumeric.java
 *
 */
@SuppressWarnings("deprecation")
@Description(name = "hdr_histogram", value = "_FUNC_(x, precision) - Returns a HDR Histogram JSON of a set of numbers", extended = "Example: \n"
    + "SELECT hdr_histogram(val, 3) from src;\n"
    // TODO insert example output representation, could do x/y like
    // HistogramNumeric.
    + "This creates a HDR histogram for the specified set, with the specified result precision")
public class GenericUDAFHdrHistogram extends AbstractGenericUDAFResolver {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFHdrHistogram.class.getName());

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
