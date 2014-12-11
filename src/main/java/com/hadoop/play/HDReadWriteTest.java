package com.hadoop.play;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDReadWriteTest implements Callable<Integer>{

	int m_numOfFiles;
	int m_filesCreated;
	boolean m_isHDTest;
	String sysPath;
	
	public HDReadWriteTest(boolean isHDTest, int numOfFiles){
		m_numOfFiles = numOfFiles;
		m_filesCreated = 0;
		m_isHDTest = isHDTest;
		if(isHDTest)
			sysPath = System.getenv("MNT_PATH");
		else
			sysPath = System.getenv("LOCAL_PATH");
	}
	
	private void mountHDFSCreateFile(String dirPath) throws Exception{

		
		String filePath = "";
		File fObj = null;
		BufferedWriter bw = null;
		for(int in=0; in < m_numOfFiles ; in++){
			filePath = in + ".txt";
			fObj = new File(dirPath, filePath);
			boolean fCreated = fObj.createNewFile();
			if(fCreated){
				System.out.println("File created..." +fObj.getAbsolutePath());
				String content = readContents();
				try{
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fObj)));
					bw.write(content);
				}finally{
					bw.close();
				}
				m_filesCreated++;
				// now write something..
			}else{
				System.out.println("File failed to create" + fObj.getAbsolutePath());
			}
		}
		
		System.out.println("mounted HDFS test done...");
	}
	
	private void mountHDFSCreateFileUsingHDFS(String dirPath) throws Exception{
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/hdfs-site.xml"));
        
		String filePath = "";
		FileSystem fs = FileSystem.get(conf);
		for(int in=0; in < m_numOfFiles ; in++){
			filePath = dirPath + "//" + in + ".txt";
			Path path = new Path(filePath);
			FSDataOutputStream out = null;
			try{
				out = fs.create(path);
				String content = readContentsUsingHDFS();
				out.writeBytes(content);
			}finally{
				out.close();
				fs.close();
			}
		}
		
		System.out.println("mounted HDFS test done...");
		
	}
	
	public String readContents() throws IOException{
		String path = sysPath + "//" + "ReadData.txt";
		BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
		StringBuffer bf = new StringBuffer();
	     
		try{
			String line=br.readLine();
			while (line != null){
	            //System.out.println(line);
	            bf.append(line);
	            line = br.readLine();
			}	
		}finally{
			br.close();
		}
		return bf.toString();
	}
	
	public String readContentsUsingHDFS() throws IOException{
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/hdfs-site.xml"));
        
		Path pt=new Path("hdfs://192.168.125.156:9000/ReadData.txt");
        //Path pt=new Path("/ReadData.txt");
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		
		StringBuffer bf = new StringBuffer();
	     
		try{
			String line=br.readLine();
			while (line != null){
	            //System.out.println(line);
	            bf.append(line);
	            line = br.readLine();
			}	
		}finally{
			br.close();
		}
		return bf.toString();
	}
	
	public boolean createDir(String thDirPath) throws Exception{
	
		System.out.println("Mnt path: " +sysPath);
		
		
		String mntPath =  sysPath + "/" + thDirPath;
		File fileObj = new File(mntPath);
		boolean b = false;
		
		if(!fileObj.exists()){
			b = fileObj.mkdirs();
			if(b)
				System.out.println("Directory created: " +mntPath);
			else
				System.out.println("Failed to create directory: " +mntPath);
		}else{
			b = true;
			System.out.println("Directory exist..");
		}
		if(b){
			mountHDFSCreateFile(mntPath);
		}
		return b;
		
	}
	
	public boolean createDirUsingHDFS(String thDirPath) throws Exception{
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/srini/hadoop-2.4.0/etc/hadoop/hdfs-site.xml"));
            
		String path = "hdfs://192.168.125.156:9000/" + thDirPath;
        //String path = "/" + thDirPath;
		Path pt=new Path(path);
		FileSystem fs = FileSystem.get(conf);
		
		if (fs.exists(pt)) {
            System.out.println("Dir " + thDirPath + " already not exists");
            return false;
        }

        fs.mkdirs(pt);

        mountHDFSCreateFileUsingHDFS(path);
        fs.close();
        return true;
	}

	public void writeToHD() throws Exception{
		
		
		long thID = Thread.currentThread().getId();
		String threadID = "TH_"   + InetAddress.getLocalHost().getHostName() + "_" + System.currentTimeMillis() + "_" + Long.toString(thID);
		if(createDir(threadID))
		//if(createDirUsingHDFS(threadID))
			System.out.println("Thread: " +threadID + " succeeded to create dir and files..");
		else
			System.out.println("Thread: " +threadID + " Failed to create dir and files..");
		
	}

	public Integer call() throws Exception {
		writeToHD();
		return m_filesCreated;
	}
	
}
