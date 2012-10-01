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
	name = "forecastDualDriver",
	description="""
""",
	supportsWindow = false,
	args = [
		@ArgDef(name="switchover", typeName="string", argTypes = [ArgType.STRING],
			description="""switchover definition. Can be a date, the string "today", or empty (treats all periods as historic)
"""
		),
	@ArgDef(name="driver1ColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the fist driver (for example count, squaremeter, amount)
"""
	),
	@ArgDef(name="driver2ColumnName", typeName="string", argTypes = [ArgType.STRING],
		description="""name of the column that stores the second driver (for example count, squaremeter, amount)
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

public class ForecastDualDriver extends AbstractTableFunction{
	
	Configuration cfg
	String switchover
	String driver1ColumnName
	String driver2ColumnName
	String periodColumnName
	String valueColumnName
	static double slope;
	static double c;
	Map<String, TypeInfo> typemap;
	
	
	//cant be inside of execute method?
	double valueperdriver1 = 0;
	double valueperdriver2 = 0;
	def pastDrivers1 = [:];
	def pastDrivers2 = [:];
	double forecastDriver1 = 0;
	double forecastDriver2 = 0;
	double forecastValue = 0;
	Date switchover_date;
	Date long_date;
	double last_historic_value = 0;
	double last_historic_driver1 = 0;
	double last_historic_driver2 = 0;
	
	@Override
	protected IPartition execute(IPartition inpPart) throws WindowingException {
		// TODO Auto-generated method stub
		
		TableFunctionOutputPartition op = new TableFunctionOutputPartition(tableFunction: this, mapSide : false)
		op.initialize(cfg)
		
		if(!switchover_date){
		 switchover_date = getSwitchOverDate();
		}
	
		
		for(Row r in inpPart){

			def driver1 = r."$driver1ColumnName"
			def driver2 = r."$driver2ColumnName"
			long long_date = (new Double(r."$periodColumnName")).longValue();
			//Timestamp period = r."$periodColumnName"
			def value  = r."$valueColumnName"
			Timestamp period = new Timestamp(long_date*1000L)
			
			
		
		//make a date out of period information
		//Date period = new Date(((Integer.parseInt(string_period.substring(1, 4)))-1900),((Integer.parseInt(string_period.substring(6, 7)))-1),(Integer.parseInt(string_period.substring(9, 10))))
			
		//if period is in the past, define value per 1 driver. put the date value pair to the pastDrivers Map
			//Long switchover_date_ts = (switchover_date/1000L);
			if(period<switchover_date){
				
			pastDrivers1.(""+((period).getTime()/(1000L*60L*60L*24L))) = driver1
			pastDrivers2.(""+((period).getTime()/(1000L*60L*60L*24L))) = driver2
			last_historic_value = value;
			last_historic_driver1 = driver1;
			last_historic_driver2 = driver2;
			
			}else{
		//if period is in the future, check if driver is filled and calculate forecastValue,
		//else, forecast driver and value
		
				if(driver1&&driver2){
					forecastValue = forecastValue(driver1, driver2)
					value = forecastValue
			}
				else{
				if(!driver1){
					forecastDriver1 = forecastDriver(pastDrivers1, period)
					driver1 = forecastDriver1}
				if(!driver2){
					forecastDriver2 = forecastDriver(pastDrivers2, period)
					driver2 = forecastDriver2}
					forecastValue = forecastValue(driver1, driver2)
					
					
					value = forecastValue
			}
					
			}
			
			double effect1 = ((driver1/last_historic_driver1)-1)*last_historic_value
			double effect2 = ((driver2/last_historic_driver2)-1)*last_historic_value
			double interaction = value - (last_historic_value+(effect1+effect2))
			//create output partition and add the row to result table
			def res = []
			typemap.each { entry ->
		
				switch(entry.key) {
				case ("$driver1ColumnName"):
					res << driver1
					break
				case ("$driver2ColumnName"):
					res << driver2
					break
				case ("$valueColumnName"):
					res << value
					break
				case ("effect"+"$driver1ColumnName"):
					res << effect1
					break
				case ("effect"+"$driver2ColumnName"):
					res << effect2
					break
				case ("interaction"):
					res << interaction
					break
				default:
				res << r.(entry.key);
				
				}
			}
			
			op << res
			

		}
			
		
		return op;
	}
	
	protected double forecastValue(double driver1, double driver2) {
		
		def forecast = driver1*driver2
		return forecast
		
	}
	
	protected Double forecastDriver(Map pastDrivers, Date period){
		

		
		//use linear regression to determine the value for the given period
        // first pass: compute xbar and ybar

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
		typemap["effect"+"$driver1ColumnName"] = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["effect"+"$driver2ColumnName"] = TypeInfoFactory.getPrimitiveTypeInfo("double")
		typemap["interaction"] = TypeInfoFactory.getPrimitiveTypeInfo("double")
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
