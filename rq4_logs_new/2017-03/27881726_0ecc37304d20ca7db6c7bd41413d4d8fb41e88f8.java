/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oulib.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Tao Zhao
 */
public class S3TiffMetadataProcessorThread implements Runnable{
    
    private S3BookInfo bookInfo;
    private String filter;
    private AmazonS3 s3client;
    private String output;
    private List<String> tiffList;

    public S3BookInfo getBookInfo() {
        return bookInfo;
    }

    public String getFilter() {
        return filter;
    }

    public AmazonS3 getS3client() {
        return s3client;
    }

    public String getOutput() {
        return output;
    }

    public List<String> getTiffList() {
        return tiffList;
    }

    public void setBookInfo(S3BookInfo bookInfo) {
        this.bookInfo = bookInfo;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setS3client(AmazonS3 s3client) {
        this.s3client = s3client;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void setTiffList(List<String> tiffList) {
        this.tiffList = tiffList;
    }
    
    public S3TiffMetadataProcessorThread(){
        
    }
    
    public S3TiffMetadataProcessorThread(AmazonS3 s3client, S3BookInfo bookInfo, String filter, List<String> tiffList){
        this.s3client = s3client;
        this.bookInfo = bookInfo;
        this.filter = filter;
        this.output = "";
        this.tiffList = tiffList;
    }
    
    @Override
    public void run (){
    	
    	String sourceBucketName = bookInfo.getBucketSourceName();
        String targetBucketName = bookInfo.getBucketTargetName();
        String bookName = bookInfo.getBookName();
        
        String bucketFolder = S3Util.S3_TIFF_METADATA_PROCESS_OUTPUT + File.separator + sourceBucketName;
        File bucketFolderFile = new File(bucketFolder);
        if(!bucketFolderFile.exists() || !bucketFolderFile.isDirectory()){
        	bucketFolderFile.mkdirs();
        }
        
        String bookPath = bucketFolder + File.separator + bookName;
        File bookFile = new File(bookPath);
        if(!bookFile.exists()){
        	bookFile.mkdir();
        }
        
        try{
        
            // Every book has a folder in the target bucket:
            Map targetBucketKeyMap = S3Util.getBucketObjectKeyMap(targetBucketName, bookName, s3client);
            if(!S3Util.folderExitsts(bookName, targetBucketKeyMap)){
                S3Util.createFolder(targetBucketName, bookName, s3client);
            }

                for (String key : tiffList) {
                    if(key.contains(".tif") && matchS3ObjKeyWithFilter(key, filter) && targetBucketKeyMap.containsKey(key+".tif")){
                        S3Object objectSource = s3client.getObject(new GetObjectRequest(sourceBucketName, key));
                        S3Object objectTarget = s3client.getObject(new GetObjectRequest(targetBucketName, key));
                        output += ("Start to copy metadata from the source object "+sourceBucketName + key+" to target object "+ targetBucketName + key + "\n");
                        System.out.println("Start to copy metadata from the source object "+sourceBucketName + key+" to target object "+ targetBucketName + key + "\n");
                        S3Util.copyS3ObjectTiffMetadata(s3client, objectSource, objectTarget); 
                        System.out.println("Finished copy metadata for the object with key="+key+"\n");
                        output += "Finished copy metadata for the object with key="+key+"\n";
                        try {
                            objectSource.close();
                            objectTarget.close();
                        } catch (IOException ex) {
                            Logger.getLogger(S3TiffProcessorThread.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
                System.out.println(Thread.currentThread().getName() + "'s job is done!");
	        
        } catch (AmazonServiceException ase) {
            output += "Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.\n";
            output += "Error Message:    " + ase.getMessage();
            output += "HTTP Status Code: " + ase.getStatusCode();
            output += "AWS Error Code:   " + ase.getErrorCode();
            output += "Error Type:       " + ase.getErrorType();
            output += "Request ID:       " + ase.getRequestId();
           System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.\n");
           System.out.println("Error Message:    " + ase.getMessage());
           System.out.println("HTTP Status Code: " + ase.getStatusCode());
           System.out.println("AWS Error Code:   " + ase.getErrorCode());
           System.out.println("Error Type:       " + ase.getErrorType());
           System.out.println("Request ID:       " + ase.getRequestId());
       } catch (AmazonClientException ace) {
           output += "Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, \nsuch as not being able to access the network.\n";
           output += "Error Message: " + ace.getMessage();
           System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, \nsuch as not being able to access the network.\n");
           System.out.println("Error Message: " + ace.getMessage());
       } finally{
            outputToFile(bookPath+File.separator+filter+".txt");
        }
    }
    
    public void outputToFile(String filePath){
        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter out = null;
        
        try{
            File file = new File(filePath);  
            File parentDir = new File(file.getParent());
            if(!parentDir.exists()){
                parentDir.mkdirs();
            }
            if(!file.exists()){
                file.createNewFile();                
            }
            
            fw = new FileWriter(filePath, true);
            bw = new BufferedWriter(fw);
            out = new PrintWriter(bw);
            out.println(output);
            
        }
        catch(FileNotFoundException ex){
            Logger.getLogger(S3TiffProcessorThread.class.getName()).log(Level.SEVERE, "Cannot save output to file.", ex);
        }
        catch(IOException ex){
            Logger.getLogger(S3TiffProcessorThread.class.getName()).log(Level.SEVERE, "Cannot save output to file.", ex);
        }
        finally{
            try {
                if(out != null)
                    out.close();
            } catch (Exception e) {
                //exception handling left as an exercise for the reader
            }
            try {
                if(bw != null)
                    bw.close();
            } catch (IOException e) {
                //exception handling left as an exercise for the reader
            }
            try {
                if(fw != null)
                    fw.close();
            } catch (IOException e) {
                //exception handling left as an exercise for the reader
            }
        }
    }
    
    private boolean matchS3ObjKeyWithFilter(String key, String filter){
        if(null == filter || "".equals(filter) || null==key || "".equals(key) || !key.contains(".tif")){
            return false;
        }
        Pattern pattern = Pattern.compile(S3Util.S3_NO_NUMBER_ENDDING_TIFF_PATTERN);
        boolean objKeyEndingWithDigit = false;
        Matcher m = pattern.matcher(key);
        if (m.find( )) {
           objKeyEndingWithDigit = true;
        }
        if(filter.equals("10.tif")){
            return objKeyEndingWithDigit != true;
        }
        else{
            return key.contains(filter);
        }
    }
}