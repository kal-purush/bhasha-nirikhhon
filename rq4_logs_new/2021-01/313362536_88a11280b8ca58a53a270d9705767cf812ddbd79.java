package nl.hro.cookbook.controller;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestHelper {

    public static HttpHeaders createHeaders(String username, String password){
        return new HttpHeaders() {{
            String auth = username + ":" + password;
            byte[] encodedAuth = org.apache.tomcat.util.codec.binary.Base64.encodeBase64(
                    auth.getBytes(StandardCharsets.US_ASCII) );
            String authHeader = "Basic " + new String( encodedAuth );
            set( "Authorization", authHeader );
        }};
    }

    public static Resource createTempFileResource(byte [] content) throws IOException {
        Path tempFile = Files.createTempFile("upload-file", ".txt");
        Files.write(tempFile, content);
        return new FileSystemResource(tempFile.toFile());
    }
}