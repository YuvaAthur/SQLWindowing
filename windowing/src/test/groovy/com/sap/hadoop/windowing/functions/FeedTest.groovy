package com.sap.hadoop.windowing.functions;

import static org.junit.Assert.*

import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import com.sap.hadoop.windowing.BaseTest

class FeedTest extends BaseTest 
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
	@Test
	void test() {
		wshell.execute("""
			from feed(tableinput(
						 recordreaderclass='com.sap.hadoop.windowing.io.TableWindowingInput',
						 keyClass='org.apache.hadoop.io.Text',
						 valueClass='org.apache.hadoop.io.Text',
						 inputPath='$basedir/data/balance',
						 inputformatClass='org.apache.hadoop.mapred.TextInputFormat',
						 serdeClass='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
						 columns = 'balance,deposit,withdraw,dat',
						 'columns.types' = 'double,double,double,double',
						 'field.delim'=';'
							)
			partition by dat
			order by dat,
				'balance',
				'deposit',
				'withdraw')
			select balance, opening, deposit, withdraw, closing, dat""")
	
		String r = outStream.toString()
		r = r.replace("\r\n", "\n")
		println r

	}
	
	
	

	
	
}