package com.sap.hadoop.windowing.functions;

import static org.junit.Assert.*

import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import com.sap.hadoop.windowing.BaseTest

class ForecastDriverTest extends BaseTest 
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
	@Test
	void testlocal() {
		wshell.execute("""
			from forecastDriver(tableinput(
						 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
						 keyClass='org.apache.hadoop.io.Text',
						 valueClass='org.apache.hadoop.io.Text',
						 inputPath='$basedir/data/employees',
						 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
						 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
						 columns = 'headcount,cost,name,dat',
						 'columns.types' = 'double,double,string,double',
						 'field.delim'=';'
							)
			partition by dat
			order by dat,
				'2012-06-02 00:00:00',
				'headcount',
				'dat',
				'cost')
			select name, headcount, dat, cost, effect""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r
		String e = "[A, 4.0, 1.3358556E9, 10.0, 0.0]\n"+
"[B, 6.0, 1.338534E9, 15.0, 0.0]\n"+
"[C, 7.935483870967801, 1.341126E9, 19.8387096774195, 4.8387096774195015]\n"+
"[D, 6.0, 1.3438044E9, 15.0, 0.0]\n"+
"[E, 8.0, 1.3464828E9, 20.0, 5.0]\n"+
"[F, 10.0, 1.3490748E9, 25.0, 10.0]\n"+
"[G, 15.0, 1.3517532E9, 37.5, 22.5]\n"+
"[, 17.809139784941863, 1.3543488E9, 44.52284946235466, 29.522849462354657]\n"+
"[I, 19.809139784941863, 1.3570272E9, 49.52284946235466, 34.52284946235466]"
		assert r == e
	}
	
	
	//@Test
	void testhdfs() {
		wshell.execute("""
			from forecastDriver(headcount
			partition by dat
			order by dat,
				'2012-06-02 00:00:00',
				'headcount',
				'dat',
				'cost')
			select name, headcount, dat, cost
		into path='/tmp/mytestout1' 
		serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
		format 'org.apache.hadoop.mapred.TextOutputFormat'""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r

	}

	
	
}