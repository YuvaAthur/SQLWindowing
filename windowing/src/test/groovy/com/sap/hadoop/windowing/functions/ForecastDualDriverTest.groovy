package com.sap.hadoop.windowing.functions;

import static org.junit.Assert.*

import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import com.sap.hadoop.windowing.BaseTest

class ForecastDualDriverTest extends BaseTest 
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
	@Test
	void testlocal() {
		wshell.execute("""
			from forecastDualDriver(tableinput(
						 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
						 keyClass='org.apache.hadoop.io.Text',
						 valueClass='org.apache.hadoop.io.Text',
						 inputPath='$basedir/data/salesfigures',
						 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
						 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
						 columns = 'sales,volume,price,dat',
						 'columns.types' = 'double,double,double,double',
						 'field.delim'=';'
							)
			partition by dat
			order by dat,
				'2012-06-02 00:00:00',
				'volume',
				'price',
				'dat',
				'sales')
			select sales, volume, price, dat, effectvolume, effectprice, interaction""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r

	}
	
	

	
	
}