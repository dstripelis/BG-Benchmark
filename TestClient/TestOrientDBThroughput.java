package orientDB;

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
		
		socialites.add("1K");
		//socialites.add("10K");
		//socialites.add("100K");
		//socialites.add("1000K");
		
		threadcount.add(10);
		threadcount.add(100);
		threadcount.add(1000);
		
		images.add(true);
		images.add(false);
		
		workloads.add("SymmetricVeryLowUpdateActions");
		workloads.add("SymmetricLowUpdateActions");
		workloads.add("SymmetricHighUpdateActions");
		
		// Create Schema
		edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-schema", "-db", "orientDB.TestDSClientA"});
		for (String socialitee : socialites){			
			String populateDB = "workloads/populateDB" + socialitee;
			for (Boolean image : images){								
				orientDB.TestDSClientA.DeleteRecords();
				System.out.println("DELETE RECORDS");
				String insertImage = "insertimage=" + image.toString();
				
				// Populate DB
				edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-load", "-db", "orientDB.TestDSClientA",
														"-P", populateDB, 
														"-p", insertImage});
				// Warm up DB
				edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-db", "orientDB.TestDSClientA", 
														"-p", "warmup=100000", 
														"-p", "warmupthreads=10"});
				
				for (Integer threadnum : threadcount){					
					for (String workload : workloads){						
						System.out.println("WORKLOAD EXECUTION: ");
						System.out.println(workload);
						String workload_file = "workloads/" + workload;						
						String usercount =  "usercount=" + Integer.toString( (Integer.parseInt(socialitee.replace("K", "")) * 1000));
						String threads = "threadcount=" + Integer.toString(threadnum);
						String maxexecutiontime = "maxexecutiontime=10";
						String finalexecutiontime = "finalexecutiontime=10";
						edu.usc.bg.BGMainClass.main(new String[]{"onetime", "-db", "orientDB.TestDSClientA",
																"-P", workload_file, 
																"-p", insertImage, 
																"-p", usercount,
																"-p", threads,
																"-p", maxexecutiontime,
																"-p", finalexecutiontime});
						System.out.println("Workload Done");
					}
				}									
			}			
		}
	System.out.println("Test OrientDB Throughput Execution is Done");	
	System.exit(0);			
	}
}
