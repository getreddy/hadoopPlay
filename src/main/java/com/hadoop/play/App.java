package com.hadoop.play;

/**
 * Hadoop cluster write test
 *
 */
public class App 
{
	
	public void launchReadingAndWriting(boolean isHDTest, int numOfThreads, int numOfFiles) throws Exception{
		
		HDService hdObj = new HDService(isHDTest, numOfThreads, numOfFiles);
		hdObj.startAndWait();
		
		//Thread.sleep(15000);
		//hdObj.stop();
	}
	
	
	
    public static void main( String[] args ) throws Exception
    {
    	int numOfThreads = 1;
    	int numOfFiles = 1;
    	int isHDTest = 1;
    	if(args != null && args.length > 2){
    		isHDTest = new Integer(args[0]).intValue();
    		numOfThreads = new Integer(args[1]).intValue();
    		numOfFiles = new Integer(args[2]).intValue();
    	}
    	App obj = new App();
    	boolean isHD = false;
    	if(isHDTest == 1)
    		isHD = true;
    	
    	obj.launchReadingAndWriting(isHD, numOfThreads, numOfFiles);
    	//HadoopClusterTest obj = new HadoopClusterTest();
    	//obj.writeToHD();
        System.out.println( "Writing and Reading to hadoop cluster done.." );
    }
}
