package com.project.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
 
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

@Configuration
public class S3Config {

	  private String awsId = "AKIAVODOQPPGRZGE7CMG";
	 
	  private String awsKey = "E2VrlOEf5TAldvCDyADODzStg6m4r83DhaX6fy1q";
	  
	  private String region = "us-east-2";
	 
	  @Bean
	  public AmazonS3 s3client() {
	    BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsId, awsKey);
	    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
	                .withRegion(Regions.fromName(region))
	                            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                            .build();
	    
	    return s3Client;
	  }
}