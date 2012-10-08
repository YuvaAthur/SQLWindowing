package com.sap.hadoop.windowing.functions;

import static org.junit.Assert.*

import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import com.sap.hadoop.windowing.BaseTest

class GrowTest extends BaseTest 
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
	@Test
	void testlinear() {
		wshell.execute("""
			from grow(tableinput(
						 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
						 keyClass='org.apache.hadoop.io.Text',
						 valueClass='org.apache.hadoop.io.Text',
						 inputPath='$basedir/data/sales',
						 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
						 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
						 columns = 'sales,growthrate,dat',
						 'columns.types' = 'double,double,double',
						 'field.delim'=';'
							)
			partition by dat
			order by dat,
				'2012-07-02 00:00:00',
				'dat',
				'sales',
				'growthrate',
				'linear')
			select sales, growthrate, dat, grow""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r

	}
	
	@Test
	void testcompound() {
		wshell.execute("""
			from grow(tableinput(
						 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
						 keyClass='org.apache.hadoop.io.Text',
						 valueClass='org.apache.hadoop.io.Text',
						 inputPath='$basedir/data/sales',
						 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
						 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
						 columns = 'sales,growthrate,dat',
						 'columns.types' = 'double,double,double',
						 'field.delim'=';'
							)
			partition by dat
			order by dat,
				'2012-07-02 00:00:00',
				'dat',
				'sales',
				'growthrate',
				'compound')
			select sales, growthrate, dat, grow""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r

	}
	
	
	

	
	
}