package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 将输入参数为“数字_数字_数字[数字_....]形式的字符串转换成多行
 * 如创意元素id串的形式为"1_2_3_4"
 * 函数名称为expode_one_col(str)
 * @author jiaqiang
 * 2013.08.14
 */
public class ExplodeOneColUDTF extends GenericUDTF {

	@Override
	public void close() throws HiveException {
		
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {	
		if (arg0.length != 1) {
			throw new UDFArgumentLengthException("expode_one_col takes only one argument");
		}
		
		if (arg0[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("expode_one_col takes string as a parameter");
		}
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
	}

	@Override
	public void process(Object[] arg0) throws HiveException {
		String input = arg0[0].toString();
		String[] fieldArr = input.split("_");
		
		forward(fieldArr);
	}

}
