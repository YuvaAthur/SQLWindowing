package com.sap.hadoop.windowing.parser;

import static org.junit.Assert.*;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.tree.CommonTree;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.sap.hadoop.windowing.BaseTest;
import com.sap.hadoop.windowing.WindowingException;
import com.sap.hadoop.windowing.query.QuerySpec;
import com.sap.hadoop.windowing.runtime.Mode;
import com.sap.hadoop.windowing.runtime.WindowingShell;

class ParserTest extends BaseTest
{
	@Test
	void test1() 
	{
		QuerySpec qSpec = wshell.parse("""
from tableinput(
			 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
       keyClass='org.apache.hadoop.io.Text', 
       valueClass='org.apache.hadoop.io.Text',
			 inputPath='$basedir/data/partsmall',
			 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
			 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
			 columns = 'p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment',
			 'columns.types' = 'int,string,string,string,string,int,string,double,string'
			  )
partition by p_mfgr
order by p_mfgr, p_name
with
  rank() as r,
  sum(p_size) over rows between unbounded preceding and current row as s,
  sum(p_size) over rows between current row and unbounded following as s1,
	min(<p_size>) over rows between 2 preceding and 2 following as m[int],
  denserank() as dr,
  cumedist() as cud,
  percentrank() as pr,
  ntile(<3>) as nt,
  count(<p_size>) as c,
  count(<p_size>, 'all') as ca,
  count(<p_size>, 'distinct') as cd,
  avg(<p_size>) as avg, stddev(p_size) as st,
	first_value(p_size) as fv, last_value(p_size) as lv,
  first_value(p_size, 'true') over rows between 2 preceding and 2 following as fv2
select p_mfgr,p_name, p_size, r, s, s1, m, dr, cud, pr, nt, c, ca, cd, avg, st, fv,lv, fv2""")
		
		//print qSpec.toString()
		assert qSpec.toString() == """Query:
	tableInput=(windowInputClass=com.sap.hadoop.windowing.io.TableWindowingInput, inputPath=$basedir/data/partsmall, keyClass=org.apache.hadoop.io.Text, valueClass=org.apache.hadoop.io.Text, inputFormatClass=org.apache.hadoop.mapred.TextInputFormat, serDeClass=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={columns=p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment, columns.types=int,string,string,string,string,int,string,double,string}
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null),
		sum(alias=s, param=[id=p_size], type=null, window=window(start=range(Unbounded PRECEDING), end=currentRow)),
		sum(alias=s1, param=[id=p_size], type=null, window=window(start=currentRow, end=range(Unbounded FOLLOWING))),
		min(alias=m, param=[expr=p_size], type=int, window=window(start=range(2 PRECEDING), end=range(2 FOLLOWING))),
		denserank(alias=dr, param=[], type=null, window=null),
		cumedist(alias=cud, param=[], type=null, window=null),
		percentrank(alias=pr, param=[], type=null, window=null),
		ntile(alias=nt, param=[expr=3], type=null, window=null),
		count(alias=c, param=[expr=p_size], type=null, window=null),
		count(alias=ca, param=[expr=p_size, strVal=all], type=null, window=null),
		count(alias=cd, param=[expr=p_size, strVal=distinct], type=null, window=null),
		avg(alias=avg, param=[expr=p_size], type=null, window=null),
		stddev(alias=st, param=[id=p_size], type=null, window=null),
		first_value(alias=fv, param=[id=p_size], type=null, window=null),
		last_value(alias=lv, param=[id=p_size], type=null, window=null),
		first_value(alias=fv2, param=[id=p_size, strVal=true], type=null, window=window(start=range(2 PRECEDING), end=range(2 FOLLOWING)))]
	select=p_mfgr, p_name, p_size, r, s, s1, m, dr, cud, pr, nt, c, ca, cd, avg, st, fv, lv, fv2
	whereExpr=null
	tableOutput=(output(path=null, serde=null, serDeProps={}, format=null)
"""	
	}
	
	@Test(expected=WindowingException.class)
	void test2()
	{
		QuerySpec qSpec = wshell.parse("""
		from tableinput(
					 columns = 'p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment',
					 'columns.types' = 'int,string,string,string,string,int,string,double,string'
						)
		order by p_mfgr, p_name
		with
			rank() as r
		select p_mfgr,p_name, p_size, r""")
	}
	
	@Test
	void testTableName()
	{
		QuerySpec qSpec = wshell.parse("""
		from part
		partition by p_mfgr
		order by p_mfgr, p_name
		with
			rank() as r
		select p_mfgr,p_name, p_size, r""")
	}
	
	@Test
	void testOutputPath()
	{
		QuerySpec qSpec = wshell.parse("""
		from part
		partition by p_mfgr
		order by p_mfgr, p_name
		with
			rank() as r
		select p_mfgr,p_name, p_size, r
		into path='/tmp/wout'""")
		//print qSpec.toString()
		assert qSpec.toString() == """Query:
	tableInput=(hiveTable=part
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null)]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=null, serDeProps={}, format=null)
"""
	}
	
	@Test
	void testOutputFormat()
	{
		QuerySpec qSpec = wshell.parse("""
		from part
		partition by p_mfgr
		order by p_mfgr, p_name
		with
			rank() as r
		select p_mfgr,p_name, p_size, r
		into path='/tmp/wout' 
		serde 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe' 
		format 'org.apache.hadoop.mapred.SequenceFileOutputFormat'""")
		//print qSpec.toString()
		assert qSpec.toString() == """Query:
	tableInput=(hiveTable=part
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null)]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe, serDeProps={}, format=org.apache.hadoop.mapred.SequenceFileOutputFormat)
"""
	}
	
	@Test
	void testEmbeddedHiveQuery()
	{
		QuerySpec qSpec = wshell.parse("""
		from <select p_mfgr, p_name, p_size 
				from part_rc>
		partition by p_mfgr
		order by p_mfgr, p_name
		with
		rank() as r
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout' 
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() == """Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size 
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null)]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat)
"""
	}
	
	@Test
	void testTableFunction()
	{
		QuerySpec qSpec = wshell.parse("""
		from npath(<select p_mfgr, p_name, p_size
				from part_rc> partition by p_mfgr order by p_mfgr, p_name, '')
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout'
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() =="""Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=npath(param=[strVal=], partitionColumns=[null], orderColumns=[null], window=null)
	funcSpecs=[]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat)
"""
	}
	
	@Test
	void testTableFunction2()
	{
		QuerySpec qSpec = wshell.parse("""
		from dummy(npath(<select p_mfgr, p_name, p_size
				from part_rc> partition by p_mfgr order by p_mfgr, p_name, ''), 'a', 'b')
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout'
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() =="""Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=dummy(npath(param=[strVal=], partitionColumns=[null], orderColumns=[null], window=null), param=[strVal=a, strVal=b], partitionColumns=[null], orderColumns=[null], window=null)
	funcSpecs=[]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat)
"""
	}
	
	@Test
	void testTableFunction3()
	{
		QuerySpec qSpec = wshell.parse("""
		from dummy(npath(<select p_mfgr, p_name, p_size
				from part_rc> partition by p_mfgr order by p_mfgr, p_name, ''), 
              'a', 'b') partition by a order by a
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout'
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() =="""Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=dummy(npath(param=[strVal=], partitionColumns=[null], orderColumns=[null], window=null), param=[strVal=a, strVal=b], partitionColumns=[a], orderColumns=[a ASC], window=null)
	funcSpecs=[]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat)
"""
	}
	
	@Test
	void testOutputToTable()
	{
		QuerySpec qSpec = wshell.parse("""
		from <select p_mfgr, p_name, p_size 
				from part_rc>
		partition by p_mfgr
		order by p_mfgr, p_name
		with
		rank() as r
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout' 
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'
		load overwrite into table part_win""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() =="""Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size 
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null)]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat, tableName=part_win, loadClause=null, overwrite=true)
"""
	}
	
	@Test
	void testOutputToTable2()
	{
		QuerySpec qSpec = wshell.parse("""
		from <select p_mfgr, p_name, p_size
				from part_rc>
		partition by p_mfgr
		order by p_mfgr, p_name
		with
		rank() as r
select p_mfgr,p_name, p_size, r
		into path='/tmp/wout'
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'
		load into table part_win partition '(p_date=\\'12-15-2010\\')'""")
		
		//println qSpec.toString();
		
		assert qSpec.toString() == """Query:
	tableInput=(hiveQuery=<select p_mfgr, p_name, p_size
				from part_rc>
		partitionColumns=p_mfgr
		orderColumns=p_mfgr ASC, p_name ASC)
	tableFuncSpec=null
	funcSpecs=[rank(alias=r, param=[], type=null, window=null)]
	select=p_mfgr, p_name, p_size, r
	whereExpr=null
	tableOutput=(output(path=/tmp/wout, serde=org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, serDeProps={}, format=org.apache.hadoop.mapred.TextOutputFormat, tableName=part_win, loadClause=(p_date=\\'12-15-2010\\'), overwrite=false)
"""
	}
}
