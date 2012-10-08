package com.sap.hadoop.windowing.functions

import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.sap.hadoop.windowing.WindowingException;
import com.sap.hadoop.windowing.functions.annotations.ArgDef;
import com.sap.hadoop.windowing.functions.annotations.FunctionDef;
import com.sap.hadoop.windowing.query.Column;
import com.sap.hadoop.windowing.query.FuncSpec;
import com.sap.hadoop.windowing.query.Query;
import com.sap.hadoop.windowing.runtime.ArgType;
import com.sap.hadoop.windowing.runtime.IPartition;
import com.sap.hadoop.windowing.runtime.OutputObj;
import com.sap.hadoop.windowing.runtime.Row;
import com.sap.hadoop.windowing.runtime.TableFunctionOutputPartition;

@FunctionDef(
	name = "timeline",
	description="""for row that meet a specified criteria, return a list of attributes that collects aggregate information about the row. 
Works on rows ordered by time. The expressions used to collect information can refer to 'timeline', this represents the entire partition that the row is in.
Functions beforeEvent() and afterEvent() indicate if a row happened before/after the row being aggregated
""",
	supportsWindow = false,
	args = [
		@ArgDef(name="criteria", typeName="script", argTypes = [ArgType.SCRIPT],
			description="""specify the criteria used to choose the rows that are being analyzed. Not all rows need to be analyzed (and hence output).
For e.g. in a timeline of web visits and orders; only rows representing ORDER events are being analyzed.
"""
		),
		@ArgDef(name="results", typeName="expression", argTypes = [ArgType.SCRIPT],
			description="""specified as a list. Each entry can be just a string, or a list of 3 elems: [expr, type, name].
If an element is just a string, it is interpreted as a reference to a column in the input to this function or as a Symbol.
When specified as a list the first element is interpreted as a groovy expression; the second is interpreted as a typename, and the
third is the expression's alias. For eg <["weight", ["2*weight", "double", 'doubleWeight"]>.
""")
	]
)
class Timeline extends AbstractTableFunction
{
	Script criteria
	Map<String, String> symbols
	List<Object> results
	ArrayList<TResColumn> outCols;
	LinkedHashMap<String, TypeInfo> outputShape
	Configuration cfg
	
	@Override
	protected IPartition execute(IPartition inpPart) throws WindowingException
	{
		TableFunctionOutputPartition op = new TableFunctionOutputPartition(tableFunction: this, mapSide : false)
		op.initialize(cfg)
		
		
		Row row = inpPart.getRowObject()
		EventRow evntRow = new EventRow(irow : row, 
			timeline : new TimelineSubPartition(tRow : new TimelineRow(inpPart, row), sz : inpPart.size()))
		evntRow.timeline.tRow.eRow = evntRow
		
		row.bind(criteria)
		outCols.each { TResColumn c ->
			evntRow.bind(c.expr)
		}

		for(r in 0..<inpPart.size())
		{
			row = inpPart[r]
			if ( criteria.run() )
			{
				def res = []
				outCols.each { TResColumn c ->
					res << c.expr.run()
				}
				op << res
			}
		}
		
		return op;
	}
	
	private void setupOutputShape(GroovyShell wshell, Query qry)
	{
		outputShape = new LinkedHashMap<String, TypeInfo>()
		outCols.each { TResColumn rc ->
			outputShape.put(rc.name, rc.typeInfo)
		}

	}
	
	protected void completeTranslation(GroovyShell wshell, Query qry, FuncSpec funcSpec) throws WindowingException
	{
		// setup ResultColumns
		outCols = new ArrayList<TResColumn>()
		results.each { r ->
			TResColumn rc = new TResColumn()
			if ( r instanceof String )
			{
				rc.name = r
				rc.typeInfo = getInputTypeInfo(qry, r)
				rc.expr = wshell.parse(r)
			}
			else
			{
				/* specified as [expr, type, name] */
				if ( r.size() != 3)
				{
					throw new WindowingException("Result Column must be specified as [expr, type, name]")
				}
				rc.name = r[2]
				rc.typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(r[1])
				rc.expr = wshell.parse(r[0])
			}
			outCols << rc
		}
		setupOutputShape(wshell, qry)
		cfg = qry.cfg
	}
	
	TypeInfo getInputTypeInfo(Query qry, String name) throws WindowingException
	{
		if ( input instanceof AbstractTableFunction )
		{
			input.getOutputShape().each { entry ->
				if ( entry.key == name) return entry.value
			}
		}
		else
		{
			 Column iCol = qry.input.columns.find { it.name == name }
			 if ( iCol ) return iCol.typeInfo
		}
		throw new WindowingException(sprintf("Unknown column %s", name))
	}
	
	@Override
	public Map<String, TypeInfo> getOutputShape()
	{
		return outputShape
	}
}

class TResColumn
{
	String name
	TypeInfo typeInfo
	Script expr
}

class TimelineSubPartition extends AbstractList<Row>
{
	TimelineRow tRow
	int sz
	
	int size() { return sz; }
	
	Object get(int i) 
	{ 
		tRow.idx = i; 
		return tRow; 
	}
	
}

class EventRow extends Row
{
	Row irow
	TimelineSubPartition timeline
	
	private static ArrayList<String> timelineVariables = ["timeline"] ;
	
	def getVariable(String name)
	{
		switch(name)
		{
			case timelineVariables:
				return timeline
			default:
				return irow.getVariable(name)
		}
	}
	
}

class TimelineRow
{
	OutputObj irow
	EventRow eRow
	int idx
	
	TimelineRow(IPartition inpPart, Row irow)
	{
		this.irow = new OutputObj(iObj : irow, p : inpPart)
	}
	
	Object getProperty(String propertyName)
	{
		return irow.getValue(idx, propertyName)
	}
	
	boolean beforeEvent() { return idx < eRow.irow.idx }
	boolean afterEvent() { return idx > eRow.irow.idx }
}
