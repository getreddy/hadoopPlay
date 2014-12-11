package com.hadoop.play;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.AbstractExecutionThreadService;


public class HDService extends AbstractExecutionThreadService{

	int m_numOfThds;
	int m_totalNumOfFiles;
	long startTime = 0;
	long endTime = 0;
	boolean m_isHDTest = true;
	
	public HDService(boolean isHDTest, int numThds, int totalNumOfFiles){
		m_numOfThds = numThds;
		m_totalNumOfFiles = totalNumOfFiles;
		m_isHDTest = isHDTest;
	}
	
	protected void startUp() throws Exception{
		startTime = System.currentTimeMillis();
		System.out.println("HDService service started...");
		
	}
	
	@Override
	protected void run() throws Exception {
		
		/*while(true && isRunning()){
			System.out.println("AbstractExecutionThreadService working..");
			Thread.sleep(5000);
		}*/
		List<Future<Integer>> list = new ArrayList<Future<Integer>>();
		ExecutorService executor = Executors.newFixedThreadPool(m_numOfThds);
		int numOfFiles = m_totalNumOfFiles;
		if(numOfFiles > m_numOfThds)
			numOfFiles = m_totalNumOfFiles / m_numOfThds;
		for(int in = 0; in < m_numOfThds; in++){
			HDReadWriteTest worker = new HDReadWriteTest(m_isHDTest, numOfFiles);
			Future<Integer> submit = executor.submit(worker);
			list.add(submit);
		}
		
		int numFiles = 0;
	    System.out.println(list.size());
	    // now retrieve the result
	    for (Future<Integer> future : list) {
	      try {
	    	  numFiles += future.get();
	      } catch (InterruptedException e) {
	        e.printStackTrace();
	      } catch (ExecutionException e) {
	        e.printStackTrace();
	      }
	    }
	    System.out.println("Number of Files written:" +numFiles);
	    executor.shutdown();
	}
	
	protected void shutDown() throws Exception{
		long endTime = System.currentTimeMillis();
		System.out.println("Time elapsed in secs: " + (endTime - startTime) / 1000);
		System.out.println("HDService service stopped...");
	}
	

}

