package com.sap.hadoop.windowing.functions;


import com.sap.hadoop.windowing.WindowingException
import com.sap.hadoop.windowing.functions.annotations.ArgDef
import com.sap.hadoop.windowing.functions.annotations.FunctionDef
import com.sap.hadoop.windowing.runtime.ArgType
import com.sap.hadoop.windowing.runtime.IPartition
import com.sap.hadoop.windowing.runtime.Row


import java.util.Map;
import com.sap.hadoop.windowing.runtime.Row;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.sap.hadoop.windowing.WindowingException;
import com.sap.hadoop.windowing.runtime.IPartition;
import groovy.lang.GroovyShell;

import java.text.SimpleDateFormat
import java.util.Iterator;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import com.sap.hadoop.windowing.WindowingException;
import com.sap.hadoop.windowing.functions.annotations.ArgDef;
import com.sap.hadoop.windowing.functions.annotations.FunctionDef;
import com.sap.hadoop.windowing.query.Column;
import com.sap.hadoop.windowing.query.FuncSpec;
import com.sap.hadoop.windowing.query.OutputColumn;
import com.sap.hadoop.windowing.query.Query;
import com.sap.hadoop.windowing.runtime.ArgType;
import com.sap.hadoop.windowing.runtime.IPartition;
import com.sap.hadoop.windowing.runtime.InputObj;
import com.sap.hadoop.windowing.runtime.Partition;
import com.sap.hadoop.windowing.runtime.Row;
import com.sap.hadoop.windowing.runtime.TableFunctionOutputPartition
import java.sql.Timestamp


@FunctionDef(
	name = "grow",
	description="""The Grow Business Function grows a base figure by a specified percentage each period. It can be compound or linear.
	result column "grow" returns the calculated growth for the period.
""",
	supportsWindow = false,
	args = [
		@ArgDef(name="switchover", typeName="string", argTypes = [ArgType.STRING],
			description="""switchover definition. Can be a date, the string "today", or empty (treats all periods as historic)
"""
		),
	@ArgDef(name="periodColumnName", typeName="string", argTypes = [ArgType.STRING],
			description="""name of the column that stores the period (for example date)"""
	),
	@ArgDef(name="baseColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the prime
"""
	),
	@ArgDef(name="growthRateColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the growth rate
"""
	),
	@ArgDef(name="growthType", typeName="string", argTypes = [ArgType.STRING],
		description="""can either be "Linear" or "Compound".
"""
	)
	
		])

public class Grow extends AbstractTableFunction{
	
	Configuration cfg
	String switchover
	String growthRateColumnName
	String growthType
	String baseColumnName
	String periodColumnName
	Map<String, TypeInfo> typemap;
	Double growResult;
	Date switchover_date;
	Date long_date;
	double last_historic_base = 0;
	double last_grow;
	
	@Override
	protected IPartition execute(IPartition inpPart) throws WindowingException {
		// TODO Auto-generated method stub
		
		TableFunctionOutputPartition op = new TableFunctionOutputPartition(tableFunction: this, mapSide : false)
		op.initialize(cfg)
		
		if(!switchover_date){
		 switchover_date = getSwitchOverDate();
		}
	
		//loop through all rows in inPart
		for(Row r in inpPart){

			def base = r."$baseColumnName"
			long long_date = (new Double(r."$periodColumnName")).longValue();
			//Timestamp period = r."$periodColumnName"
			Double growthrate  = r."$growthRateColumnName"
			Timestamp period = new Timestamp(long_date*1000L)
			
			
		
 
			//check if period is in the past
			//Long switchover_date_ts = (switchover_date/1000L);
			if(period<switchover_date){
				
			//in past periods grow = base
			growResult = base;
			//update last_historic_base and last_grow, which are needed to calculate grow in future periods
			last_historic_base = base;
			last_grow = base;
			}else{
		    //future period
			//calculate grow
				if(growthType.equalsIgnoreCase("linear")){
					growResult = (last_historic_base*growthrate)+last_grow;
					last_grow = growResult;
					
			}
				else if(growthType.equalsIgnoreCase("compound")){
					growResult = (last_grow*(1+growthrate));
					last_grow = growResult;
					
			}
					
			}
			
			//create output partition and add the row to result table
			def res = []
			typemap.each { entry ->
		
				switch(entry.key) {
				case ("grow"):
					res << growResult
					break
				break
				default:
				res << r.(entry.key);
				
				}
			}
			
			op << res
			

		}
			
		
		return op;
	}
	
	
	protected void completeTranslation(GroovyShell wshell, Query qry, FuncSpec funcSpec) throws WindowingException
	{
		if ( input == null || ! (input instanceof AbstractTableFunction))
		{
			typemap = [:]
			qry.input.columns.each { Column c ->
				typemap[c.field.fieldName] = TypeInfoUtils.getTypeInfoFromObjectInspector(c.field.fieldObjectInspector)
	
			}	

				
			
				
		}
		else
		{
			typemap = [:]
			typemap.addAll( ((AbstractTableFunction)input).getOutputShape());
		}
		
		//add grow to typemap
		TypeInfo ti = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["grow"] = ti
		cfg = qry.cfg
	}
	
	
	protected Date getSwitchOverDate(){
		
		//if switchover is empty, return max date
		if(!switchover){
			Date maxdate = new Date(Long.MAX_VALUE);
			return(maxdate)
			}
		
		if(switchover.equalsIgnoreCase("today")){
		//if switchover is today, return todays date
			
			def switchover_date = new Date()
			return(switchover_date)
			}
		//check if switchover is valid date and return 
		try{
		Date switchover_date = new Date(((Integer.parseInt(switchover.substring(0, 4))-1900)),((Integer.parseInt(switchover.substring(5, 7)))-1),(Integer.parseInt(switchover.substring(8, 10))));

		return switchover_date;
		}
		catch(Exception e){
		}
		

		

		
		
		}
	
	

	@Override
	public Map<String, TypeInfo> getOutputShape() {
		// TODO Auto-generated method stub
		return typemap;
	}

}
