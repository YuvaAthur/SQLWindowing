package com.sap.hadoop.windowing.runtime2;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

public class RuntimeUtils
{
	public static int compare(Object[] o1, ObjectInspector[] oi1, Object[] o2, ObjectInspector[] oi2)
	{
		int c = 0;
		for(int i=0; i<oi1.length; i++)
		{
			c = ObjectInspectorUtils.compare(o1[i], oi1[i], o2[i], oi2[i]);
			if ( c!= 0) return c;
		}
		return c;
	}
	
	public static Object[] copyToStandardObject(Object[] o, ObjectInspector[] oi, ObjectInspectorCopyOption objectInspectorOption)
	{
		Object[] out = new Object[o.length];
		for(int i=0; i<oi.length; i++)
		{
			out[i] = ObjectInspectorUtils.copyToStandardObject(o[i], oi[i], objectInspectorOption);
		}
		return out;
	}
}
