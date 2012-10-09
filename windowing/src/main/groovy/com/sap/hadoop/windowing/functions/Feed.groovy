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
import groovy.lang.GroovyShell

import java.util.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils

import com.sap.hadoop.windowing.WindowingException
import com.sap.hadoop.windowing.functions.annotations.ArgDef
import com.sap.hadoop.windowing.functions.annotations.FunctionDef
import com.sap.hadoop.windowing.query.Column
import com.sap.hadoop.windowing.query.FuncSpec
import com.sap.hadoop.windowing.query.Query
import com.sap.hadoop.windowing.runtime.ArgType
import com.sap.hadoop.windowing.runtime.IPartition
import com.sap.hadoop.windowing.runtime.Row
import com.sap.hadoop.windowing.runtime.TableFunctionOutputPartition


@FunctionDef(
	name = "feed",
	description="""Feed calculates the closing balance and "feeds" it to the opening balance of the next time period. Seems simple - but without this function, calculation rules have to be hand crafted and maintained all over the application

In the first period only, the opening balance defined as Prime. This parameter can either be a constant or a Field item. Thereafter, the opening balance assumes the closing balance of the previous period.

Results are displayed in columns "Opening" and "Closing".
""",
	supportsWindow = false,
	args = [

	@ArgDef(name="primeColumnName", typeName="string", argTypes = [ArgType.STRING],
			description="""name of the column that stores the start balance"""
	),
	@ArgDef(name="inColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the incoming values"""
	),	
	@ArgDef(name="outColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the outgoing values"""
	)
	
		])

public class Feed extends AbstractTableFunction{
	
	Configuration cfg
	String primeColumnName
	String inColumnName
	String outColumnName
	Map<String, TypeInfo> typemap;
	Double lastClosing = 0;

	
	@Override
	protected IPartition execute(IPartition inpPart) throws WindowingException {
		// TODO Auto-generated method stub
		
		TableFunctionOutputPartition op = new TableFunctionOutputPartition(tableFunction: this, mapSide : false)
		op.initialize(cfg)
		

		//loop through all rows in inPart
		for(Row r in inpPart){

			Double opening;
			Double input = r."$inColumnName";
			Double output = r."$outColumnName";
			if(r."$primeColumnName"){
				
				try{
				lastClosing = (r."$primeColumnName" + input - output)}
				catch(Exception e){
				if(!input)
				lastClosing = r."$primeColumnName" - output
				if(!output)
				lastClosing = r."$primeColumnName" + input
				if(!output&&!input)
				lastClosing = r."$primeColumnName"
				}
				
			}else{
			

				opening = lastClosing;
				lastClosing = (opening + input - output)
			
			}
			//create output partition and add the row to result table
			def res = []
			typemap.each { entry ->
		
				switch(entry.key) {
				case ("opening"):
					res << opening
					break
				case ("closing"):
					res << lastClosing
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
		
		//add opening and closing to typemap
		TypeInfo ti = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["closing"] = ti
		ti = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["opening"] = ti

		cfg = qry.cfg
	}
	
	

	@Override
	public Map<String, TypeInfo> getOutputShape() {
		// TODO Auto-generated method stub
		return typemap;
	}

}
