package com.example.the3bits.controller;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.example.the3bits.facade.user.UserFacade;
import com.example.the3bits.facade.user.model.UserResponseModel;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;

@RestController
public class FileController {
    private final UserFacade userFacade;
    private final String ACCESS_KEY = "AKIAR6HC55A5TG2KBK73";
    private final String SECRET_ACCESS_KEY = "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";
    private final AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY,SECRET_ACCESS_KEY);
    private final AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(credentials);
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(awsStaticCredentialsProvider)
            .withRegion(Regions.US_EAST_2)
            .build();


    public FileController(UserFacade userFacade) {
        this.userFacade = userFacade;
    }

    @PutMapping (value = "/userImage", consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    public ResponseEntity<UserResponseModel> uploadUserImage(@RequestParam("userImage") MultipartFile userImage, Authentication authentication) throws IOException{
        Long userId = userFacade.getIdByAuthentication(authentication);
        InputStream inputStream = userImage.getInputStream();

        PutObjectRequest requestFile = new PutObjectRequest("the3bits", userImage.getOriginalFilename(), inputStream, new ObjectMetadata());
        requestFile.withCannedAcl(CannedAccessControlList.PublicRead);
        PutObjectResult putObjectResult = s3Client.putObject(requestFile);
        String pictureURL = s3Client.getUrl("the3bits", userImage.getOriginalFilename()).toExternalForm();
        return ResponseEntity.ok(userFacade.updateImage(userId, pictureURL));
    }
}