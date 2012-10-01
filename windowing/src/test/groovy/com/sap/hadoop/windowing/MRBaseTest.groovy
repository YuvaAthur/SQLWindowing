package com.sap.hadoop.windowing;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.sap.hadoop.windowing.runtime.mr.MRTranslator;
import com.sap.hadoop.windowing.runtime.mr.MRExecutor;
import com.sap.hadoop.windowing.runtime.Mode;
import com.sap.hadoop.windowing.runtime.QueryOutputPrinter;
import com.sap.hadoop.windowing.runtime.TestExecutor;
import com.sap.hadoop.windowing.runtime.ThriftBasedHiveQueryExecutor;
import com.sap.hadoop.windowing.runtime.WindowingShell;

class CommandLog extends QueryOutputPrinter
{
	PrintStream out;
	int limit;//TODO: limits number of lines captured.
	protected printOutput(String s) { out.println(s)}
}

abstract class MRBaseTest
{
	static WindowingShell wshell;
	static basedir = "src/test/groovy"
	static ByteArrayOutputStream outStream;

	//test validation constants

	public static final String TEST_GIT_PATH = "c:/git/"
	public static final String TEST_DATA_PATH = "SQLWindowing/windowing/src/test/groovy/testOutput/"
	
	public static final String TEST_DIFF_EXE = "c:/cygwin/bin/diff.exe " 
	public static final String TEST_DIFF_EXE_PARAM = "windowing.test.diff.exe"
	
	// 0: diff with golden output | 1: no diff - don't generate local output | 2: generate golden output (check-in after this!)
	public static final Integer TEST_DIFF_MODE_DIFF_DEFAULT = 0 // 0 : default: diff with golden output
	public static final Integer TEST_DIFF_MODE_NO_DIFF = 1 // 1 : diff with golden output
	public static final Integer TEST_DIFF_MODE_GEN_REF = 2 // 2 : diff with golden output
	public static final String TEST_DIFF_MODE_PARAM = "windowing.test.diff.mode"
	
	static refDir = TEST_GIT_PATH+ TEST_DATA_PATH + "ref/"
	static tempDir = TEST_GIT_PATH+ TEST_DATA_PATH + "out/"
	
	@BeforeClass
	public static void setupClass()
	{
		outStream = new ByteArrayOutputStream()
		
		Configuration conf =  WORK_LOCALMR();
		conf.setBoolean(Constants.WINDOWING_TEST_MODE, true)
		HiveConf hCfg = new HiveConf(conf, conf.getClass())
		
		wshell = new WindowingShell(hCfg, new MRTranslator(), 
			new MRExecutor())
		wshell.hiveQryExec = new ThriftBasedHiveQueryExecutor(conf)
		
	}
	
	@Before
	public void setup()
	{
		outStream.reset();
	}

	def  testExecute(String command) // e.g. testExecute("testRC",query_string)
	{
		// Modes of execution
		//  0: Run with Diff
		//	1: Golden Output creation
		//  2: Run without Diff (no local output --> CommandLog == null
		
		def diffmode = new Integer(wshell.cfg.get(TEST_DIFF_MODE_PARAM))

		def tr = new Exception().getStackTrace()
		def methName = "" // new Exception().getStackTrace()[14].methodName
		def className =  "" //new Exception().getStackTrace()[14].className

		for (i in (tr.size()-1)..0){
			def s = tr[i].className
			if(s.contains("windowing")){
				methName = tr[i].methodName
				className = s
				break
			}
		}
		def refFile = (refDir + className + "_"+ methName + ".out")		
		def outFile = (tempDir + className + "_"+ methName + ".out")
		def sp = " "
		
		
		switch (diffmode){
			case 1: //1: no diff - don't generate local output 
				wshell.execute(command)
				break
				
			case 2: //2: generate golden output (check-in after this!)
				// TODO: check if the files have to be deleted before executing
				def cqop = new CommandLog(out: new PrintStream(refFile)) // create the output file
				wshell.execute(command, cqop)
				break
				
			default: //default : diffmode = 0
				def cqop = new CommandLog(out: new PrintStream(outFile)) // create the output file
				wshell.execute(command, cqop)

				def cmd = wshell.cfg.get(TEST_DIFF_EXE_PARAM)
				def cmdLine = cmd + refFile + sp + outFile
				def sout = new StringBuffer()
				def serr = new StringBuffer()
				def proc = cmdLine.execute()
				proc.consumeProcessOutput(sout, serr)
				proc.waitForOrKill(1000) //TODO: Tune the wait time for large files
				// print only if the output buffer is non-empty or not-null
				Assert.assertFalse((String)sout, sout)
		}
		
		
	}

		
	public static Configuration WORK()
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hbserver1.dhcp.pal.sap.corp:8020");
		conf.set("mapred.job.tracker", "hbserver1.dhcp.pal.sap.corp:8021");
		
		conf.set("hive.metastore.uris", "thrift://hbserver7.dhcp.pal.sap.corp:9083");
		//conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.metastore.local", "false");
		conf.set("windowing.jar.file", "C:/git/SQLWindowing/windowing/target/com.sap.hadoop.windowing-0.0.2-SNAPSHOT.jar");
		conf.set(" mapred.reduce.tasks", "8");
		
		conf.set(Constants.HIVE_THRIFTSERVER, "hbserver7.dhcp.pal.sap.corp")
		conf.setInt(Constants.HIVE_THRIFTSERVER_PORT, 10000)
		
		conf.set("HIVE_HOME", "C:/hive-0.9.0")
		//conf.set("HIVE_HOME", "C:/git/hive/bin/hive-0.9.0-bin")
		
		conf.set("hadoop.job.ugi", "hbutani,users");
		
		return conf;
	}
	
	public static Configuration HOME()
	{
		Configuration conf = new Configuration();
		/*conf.addResource(new URL("file:///media/MyPassport/hadoop/home-configuration/hadoop-site.xml"))
		conf.addResource(new URL("file:///media/MyPassport/hadoop/home-configuration/hdfs-site.xml"))
		conf.addResource(new URL("file:///media/MyPassport/hadoop/home-configuration/mapred-site.xml"))
		conf.addResource(new URL("file:///media/MyPassport/hadoop/home-configuration/hive-site.xml"))*/
		
		conf.set("fs.default.name", "hdfs://localhost:8020");
		conf.set("mapred.job.tracker", "localhost:8021");
		
		conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.metastore.local", "false");
		
		conf.set("windowing.jar.file", "/media/MyPassport/windowing/windowing/target/com.sap.hadoop.windowing-0.0.2-SNAPSHOT.jar");
		conf.set(" mapred.reduce.tasks", "4");
		
		conf.set(Constants.HIVE_THRIFTSERVER, "localhost")
		conf.setInt(Constants.HIVE_THRIFTSERVER_PORT, 10000)
		
		conf.set("HIVE_HOME", "/media/MyPassport/hadoop/hive-0.9.0-bin")
		
		return conf;
	}
	
	public static Configuration HOME_LOCALMR()
	{
		Configuration conf = new Configuration();
		
		/*conf.addResource("/media/MyPassport/hadoop/hime-configuration/hadoop-site.xml")
		conf.addResource("/media/MyPassport/hadoop/hime-configuration/hdfs-site.xml")
		//conf.addResource("/media/MyPassport/hadoop/hime-configuration/mapred-site.xml")
		conf.addResource("/media/MyPassport/hadoop/hime-configuration/hive-site.xml")*/
		
		conf.set("fs.default.name", "hdfs://localhost:8020");
		//conf.set("mapred.job.tracker", "localhost:8021");
		
		conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.metastore.local", "false");
		
		conf.set("windowing.jar.file", "/media/MyPassport/windowing/windowing/target/com.sap.hadoop.windowing-0.0.2-SNAPSHOT.jar");
		//conf.set(" mapred.reduce.tasks", "4");
		
		conf.set(Constants.HIVE_THRIFTSERVER, "localhost")
		conf.setInt(Constants.HIVE_THRIFTSERVER_PORT, 10000)
		conf.set("HIVE_HOME", "/media/MyPassport/hadoop/hive-0.9.0-bin")
		
		return conf;
	}
	
	public static Configuration WORK_LOCALMR()
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hbserver1.dhcp.pal.sap.corp:8020");
		//conf.set("mapred.job.tracker", "hbserver1.dhcp.pal.sap.corp:8021");
		
		conf.set("hive.metastore.uris", "thrift://hbserver7.dhcp.pal.sap.corp:9083");
		//conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.metastore.local", "false");
		conf.set("windowing.jar.file", "C:/git/SQLWindowing/windowing/target/com.sap.hadoop.windowing-0.0.2-SNAPSHOT.jar");
		
		conf.set(Constants.HIVE_THRIFTSERVER, "hbserver7.dhcp.pal.sap.corp")
		conf.setInt(Constants.HIVE_THRIFTSERVER_PORT, 10000)
		conf.set("HIVE_HOME", "C:/git/hive/bin/hive-0.9.0-bin")
		
		//for diff output
		conf.set(TEST_DIFF_EXE_PARAM, TEST_DIFF_EXE)
		//conf.setInt(TEST_DIFF_MODE_PARAM, TEST_DIFF_MODE_DIFF_DEFAULT)
		conf.setInt(TEST_DIFF_MODE_PARAM, TEST_DIFF_MODE_GEN_REF)
		
		return conf;
	}
	
}

