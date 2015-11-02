package orientDB;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.Permission;
import java.util.ArrayList;

/**
 * 
 * @author Dstrip
 *
 * In order to run the TestOrientDBThroughput.main one has to comment out the line 42: System.exit(0); 
 * inside the BGMainClass.main function so that the the BGMainClass.main function can be called repeatedly 
 * on one-time mode.
 */

public class TestOrientDBThroughput {

	public static void main(String []args){

		ArrayList <String> socialites = new ArrayList<String>(); 
		ArrayList <Integer> threadcount = new ArrayList<Integer>();
		ArrayList <Boolean> images = new ArrayList<Boolean>();
		ArrayList <String> workloads = new ArrayList<String>();
		
		//socialites.add("1K");
		//socialites.add("10K");
		socialites.add("100K");
		//socialites.add("1000K");
		
		threadcount.add(10);
		threadcount.add(100);
		threadcount.add(1000);
		
		images.add(true);
		images.add(false);
		
		workloads.add("SymmetricVeryLowUpdateActions");
		workloads.add("SymmetricLowUpdateActions");
		workloads.add("SymmetricHighUpdateActions");
		
		String java_exe = "java -Xmx1G -cp /Users/Dstrip/Desktop/BGBenchmark/BGv0.1.4776/bin:"
						+ "/Users/Dstrip/Desktop/BGBenchmark/BGv0.1.4776/lib/*:"
						+ "/Users/Dstrip/Desktop/BGBenchmark/BGv0.1.4776/db/OrientDS/lib/* "
						+ "edu.usc.bg.BGMainClass";
		
		// Create Schema
		edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-schema", "-db", "orientDB.TestDSClientA"});
		System.out.println("********************************************************************************************************");
		System.out.println("");
		for (String socialitee : socialites){			
			String populateDB = "workloads/populateDB" + socialitee;
			for (Boolean image : images){											
				String insertImage = "insertimage=" + image.toString();				
				for (Integer threadnum : threadcount){					
					for (String workload : workloads){		

						// Delete Records
						System.out.println("DELETE RECORDS");
						orientDB.TestDSClientA.DeleteRecords();	
						
						// Populate DB
						edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-load", "-db", "orientDB.TestDSClientA",
																"-P", populateDB, 
																"-p", insertImage,
																"-p","threadcount=1"});
						
						System.out.println("WORKLOAD EXECUTION: ");
						System.out.println(workload);
						String workload_file = "workloads/" + workload;						
						String usercount =  "usercount=" + Integer.toString( (Integer.parseInt(socialitee.replace("K", "")) * 1000));
						String threads = "threadcount=" + Integer.toString(threadnum);
						String maxexecutiontime = "maxexecutiontime=180";
						String finalexecutiontime = "finalexecutiontime=180";
						try {
							runProcess(java_exe
									+ " onetime -db orientDB.TestDSClientA" 
									+ " -P " + workload_file 
									+ " -p " + insertImage 
									+ " -p " + usercount 
									+ " -p warmup=100000 -p warmupthreads=100" 
									+ " -p " + threads
									+ " -p " + maxexecutiontime 
									+ " -p " + finalexecutiontime);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						System.out.println("Workload Done");
						System.out.println("");
						System.out.println("********************************************************************************************************");
						System.out.println("");
						System.out.println("NEW EXPERIMENT");

					}
				}									
			}			
		}
		System.out.println("Test OrientDB Throughput Execution is Done");	
		System.exit(0);			
	}
	
	private static void runProcess(String command) throws Exception {
		Process pro = Runtime.getRuntime().exec(command);
		printLines(command + " stdout:", pro.getInputStream());
		printLines(command + " stderr:", pro.getErrorStream());
		pro.waitFor();
		System.out.println(command + " exitValue() " + pro.exitValue());
	}

	private static void printLines(String name, InputStream ins) throws Exception {
		String line = null;
		BufferedReader in = new BufferedReader(new InputStreamReader(ins));
		while ((line = in.readLine()) != null) {
			System.out.println(name + " " + line);
		}
	}
	  
}


