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
	name = "forecastDriver",
	description="""
""",
	supportsWindow = false,
	args = [
		@ArgDef(name="switchover", typeName="string", argTypes = [ArgType.STRING],
			description="""switchover definition. Can be a date, the string "today", or empty (treats all periods as historic)
"""
		),
	@ArgDef(name="driverColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the driver (for example count, squaremeter, amount)
"""
	),
	@ArgDef(name="periodColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the period (for example date)
"""
	),
	@ArgDef(name="valueColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the value (for example cost, time)
"""
	)
	
		])

public class ForecastDriver extends AbstractTableFunction{
	
	Configuration cfg
	String switchover
	String driverColumnName
	String periodColumnName
	String valueColumnName
	static double slope;
	static double c;
	Map<String, TypeInfo> typemap;
	
	
	static double valueperdriver = 0;
	static def pastDrivers = [:];
	static double forecastDriver = 0;
	static double forecastValue = 0;
	Date switchover_date;
	Date long_date;
	double last_historic_value = 0;
	
	@Override
	protected IPartition execute(IPartition inpPart) throws WindowingException {
		// TODO Auto-generated method stub
		
		TableFunctionOutputPartition op = new TableFunctionOutputPartition(tableFunction: this, mapSide : false)
		op.initialize(cfg)
		
		if(!switchover_date){
		 switchover_date = getSwitchOverDate();
		}
	
		
		for(Row r in inpPart){

			def driver = r."$driverColumnName"
			long long_date = (new Double(r."$periodColumnName")).longValue();
			//Timestamp period = r."$periodColumnName"
			def value  = r."$valueColumnName"
			Timestamp period = new Timestamp(long_date*1000L)
			
			
		
		//make a date out of period information
		//Date period = new Date(((Integer.parseInt(string_period.substring(1, 4)))-1900),((Integer.parseInt(string_period.substring(6, 7)))-1),(Integer.parseInt(string_period.substring(9, 10))))
			
		//if period is in the past, define value per 1 driver. put the date value pair to the pastDrivers Map
			//Long switchover_date_ts = (switchover_date/1000L);
			if(period<switchover_date){
				
			try{
			valueperdriver = value/driver
			}
			catch(NullPointerException e){
				valueperdriver = 0}

			pastDrivers.(""+((period).getTime()/(1000L*60L*60L*24L))) = driver
			last_historic_value = value;
			}else{
		//if period is in the future, check if driver is filled and calculate forecastValue,
		//else, forecast driver and value
		
				if(driver){
					forecastValue = forecastValue(driver, valueperdriver)
					value = forecastValue
			}
				else{
					forecastDriver = forecastDriver(pastDrivers, period)
					forecastValue = forecastValue(forecastDriver, valueperdriver)
					driver = forecastDriver
					value = forecastValue
					
			}
					
			}
			
			double effect = value - last_historic_value;
			//create output partition and add the row to result table
			def res = []
			typemap.each { entry ->
		
				switch(entry.key) {
				case ("$driverColumnName"):
					res << driver
					break
				case ("$valueColumnName"):
					res << value
					break
				case ("effect"):
					res << effect
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
	
	protected double forecastValue(double driver, double valueperdriver) {
		
		def forecast = driver*valueperdriver
		return forecast
		
	}
	
	protected Double forecastDriver(Map pastDrivers, Date period){
		

		
		//use linear regression to determine the value for the given period
        // first pass: compute xbar and ybar
		if(!slope){
        double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;
        
        pastDrivers.each { entry -> 
            sumy += entry.value  
            sumx += Double.parseDouble( entry.key )
            sumx2 +=  Double.parseDouble( entry.key ) *  Double.parseDouble( entry.key )
            }
            
        double xbar = sumx / pastDrivers.size();
        double ybar = sumy / pastDrivers.size();        
        // second pass: compute summary statistics
        double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
        
        pastDrivers.each { entry ->
            xxbar += ((Double.parseDouble( entry.key ) - xbar)* ((Double.parseDouble( entry.key ) - xbar)))
            xybar += ((Double.parseDouble( entry.key )  - xbar)* (entry.value - xbar))
            }
        
         slope = xybar / xxbar;
         c = ybar - slope * xbar;

        // slope and c are interpreted like this: "y   =  slope*x + c);
		}
		if(pastDrivers.size()<2){
			slope = 0;
			c = Double.parseDouble( pastDrivers[0].value );
			}
		return (((period.getTime()/(1000L*60L*60L*24L))*slope)+c)
		
	}
	
	
	protected void completeTranslation(GroovyShell wshell, Query qry, FuncSpec funcSpec) throws WindowingException
	{
		if ( input == null || ! (input instanceof AbstractTableFunction))
		{
			typemap = [:]
			qry.input.columns.each { Column c ->
				typemap[c.field.fieldName] = TypeInfoUtils.getTypeInfoFromObjectInspector(c.field.fieldObjectInspector)
	
			}	//add effect to typemap

				
			
				
		}
		else
		{
			typemap = [:]
			typemap.addAll( ((AbstractTableFunction)input).getOutputShape());
		}
		
		TypeInfo ti = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["effect"] = ti
		cfg = qry.cfg
	}
	
	
	protected Date getSwitchOverDate(){
		
		//if switchover is empty, return max date
		if(!switchover){
			Date maxdate = new Date(Long.MAX_VALUE);
			return(maxdate)
			}
		
		if(switchover.equalsIgnoreCase("today")){
			
			def switchover_date = new Date()
			return(switchover_date)
			}
		//check if switchover is valid date and return 
		try{
		Date switchover_date = new Date(((Integer.parseInt(switchover.substring(0, 4))-1900)),((Integer.parseInt(switchover.substring(5, 7)))-1),(Integer.parseInt(switchover.substring(8, 10))));
		//println((Integer.parseInt(switchover.substring(0, 4))-1900));
		//println((Integer.parseInt(switchover.substring(5, 7)))-1);
		//println(Integer.parseInt(switchover.substring(8, 10)));
		//println(switchover_date);
		return switchover_date;
		}
		catch(Exception e){
		}
		
		//if switchover is today, return todays date

		

		
		
		}
	
	

	@Override
	public Map<String, TypeInfo> getOutputShape() {
		// TODO Auto-generated method stub
		return typemap;
	}

}
