package com.brainstrom.meokjang.deal.service;

import com.brainstrom.meokjang.deal.dto.request.DealRequest;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@Transactional
public class BucketService {

    private final ObjectStorage objectStorage;
    private final String ociNamespace;
    private final String ociBucketName;

    public BucketService(@Value("${OCI_CONFIG_PATH}") String ociConfigPath,
                         @Value("${OCI_NAMESPACE}") String ociNamespace,
                         @Value("${OCI_BUCKET_NAME}") String ociBucketName) throws IOException {
        try {
            ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse(ociConfigPath, "DEFAULT");
            AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile);
            objectStorage = ObjectStorageClient.builder()
                    .region(Region.AP_CHUNCHEON_1)
                    .build(provider);

//            ------------------ OCI SDK TEST ------------------
            GetNamespaceResponse namespaceResponse = objectStorage.getNamespace(GetNamespaceRequest.builder().build());
            String namespaceName = namespaceResponse.getValue();
            System.out.println("namespaceName: " + namespaceName);
            List<GetBucketRequest.Fields> fieldsList = new ArrayList<>(2);
            fieldsList.add(GetBucketRequest.Fields.ApproximateCount);
            fieldsList.add(GetBucketRequest.Fields.ApproximateSize);

            GetBucketRequest getBucketRequest = GetBucketRequest.builder()
                    .namespaceName(ociNamespace)
                    .bucketName(ociBucketName)
                    .fields(fieldsList)
                    .build();
            System.out.println("Fetching Bucket Details");
            GetBucketResponse getBucketResponse = objectStorage.getBucket(getBucketRequest);
            System.out.println("Bucket Name: " + getBucketResponse.getBucket().getName());
            System.out.println("Bucket Compartment Id: " + getBucketResponse.getBucket().getCompartmentId());
            System.out.println("Bucket Size: " + getBucketResponse.getBucket().getApproximateSize());
            System.out.println("Bucket Object Count: " + getBucketResponse.getBucket().getApproximateCount());
//            ------------------ OCI SDK TEST ------------------

        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
        this.ociNamespace = ociNamespace;
        this.ociBucketName = ociBucketName;
    }

    String[] uploadImage(DealRequest dealRequest) throws IOException {
        String[] imageList = new String[4];
        if (dealRequest.getImage1() != null) {
            imageList[0] = upload(dealRequest.getImage1());
        }
        if (dealRequest.getImage2() != null) {
            imageList[1] = upload(dealRequest.getImage2());
        }
        if (dealRequest.getImage3() != null) {
            imageList[2] = upload(dealRequest.getImage3());
        }
        if (dealRequest.getImage4() != null) {
            imageList[3] = upload(dealRequest.getImage4());
        }
        return imageList;
    }

    private String upload(MultipartFile image) throws IOException {
        UploadConfiguration uploadConfiguration =
                UploadConfiguration.builder()
                        .allowMultipartUploads(true)
                        .allowParallelUploads(true)
                        .build();
        UploadManager uploadManager = new UploadManager(objectStorage, uploadConfiguration);
        String fileName = UUID.randomUUID().toString();
        PutObjectRequest por = PutObjectRequest.builder()
                .bucketName(ociBucketName)
                .namespaceName(ociNamespace)
                .objectName(fileName)
                .contentType(image.getContentType())
                .opcMeta(null)
                .build();
        File file = convertMultipartFileToFile(image);
        if (file == null) {
            return null;
        }
        image.transferTo(file);
        UploadManager.UploadRequest uploadDetails =
                UploadManager.UploadRequest.builder(file)
                        .allowOverwrite(true)
                        .build(por);
        UploadManager.UploadResponse uploadResponse = uploadManager.upload(uploadDetails);
        System.out.println("uploadResponse: " + uploadResponse);
        file.delete();
        return fileName;
    }

    private File convertMultipartFileToFile(MultipartFile image) {
        if (image.isEmpty()) {
            return null;
        }
        File file = new File(Objects.requireNonNull(image.getOriginalFilename()));
        try {
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(image.getBytes());
            fos.close();
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
        return file;
    }

    boolean deleteImage(String fileName) {
        try {
            objectStorage.deleteObject(
                    com.oracle.bmc.objectstorage.requests.DeleteObjectRequest.builder()
                            .namespaceName(ociNamespace)
                            .bucketName(ociBucketName)
                            .objectName(fileName)
                            .build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}