package com.paula9t9.taskmaster;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.xml.ws.ServiceMode;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

@Service
public class S3Client {

    private AmazonS3 s3Client;

    @Value("${amazon.s3.endpoint}")
    private String endpointUrl;

    @Value("${amazon.aws.accesskey}")
    private String accessKey;

    @Value("${amazon.aws.secretkey}")
    private String secretKey;

    @Value("${amazon.aws.bucket}")
    private String bucket;

    @PostConstruct
    private void initializeAmazon(){
        AWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
        // TODO: refactor to match dynamodb config's nondeprecated method
        this.s3Client = new AmazonS3Client(credentials);
    }

    public String uploadFile(MultipartFile multipartFile){

        String fileUrl = "";

        try{
            File file = convertMultipartToFile(multipartFile);
            String fileName = generateFileName(multipartFile);
            fileUrl = endpointUrl + "/" + fileName;
            uploadFileToS3Bucket(fileName, file);
            file.delete();
        } catch(Exception e){
            e.printStackTrace();
        }

        return fileUrl;
    }

    private void uploadFileToS3Bucket(String fileName, File file) {
        s3Client.putObject(new PutObjectRequest(bucket, fileName, file)
                .withCannedAcl(CannedAccessControlList.PublicRead));
    }

    private String generateFileName(MultipartFile multipartFile) {
        return new Date().getTime() + "-" + multipartFile.getOriginalFilename().replace(" ","-");
    }

    private File convertMultipartToFile(MultipartFile multipartFile) throws IOException {

        File convertFile = new File(multipartFile.getOriginalFilename());
        FileOutputStream fileOutputStream = new FileOutputStream(convertFile);

        fileOutputStream.write(multipartFile.getBytes());
        fileOutputStream.close();

        return convertFile;
    }

    public String deleteFileFromS3Bucket(String fileUrl){
        String fileName = fileUrl.substring(fileUrl.lastIndexOf("/")+1);
        s3Client.deleteObject(new DeleteObjectRequest(bucket, fileName));
        return "Successfully Deleted.";
    }

}