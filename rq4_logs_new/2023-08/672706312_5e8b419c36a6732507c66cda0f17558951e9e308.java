package com.openpromt.coffeee.swf2023.openpromtserver.ipfs.service;

import com.openpromt.coffeee.swf2023.openpromtserver.ipfs.util.JsonToFileWriter;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.openpromt.coffeee.swf2023.openpromtserver.ipfs.util.FileToMultipartFileConverter.convertFileToMultipartFile;
import static com.openpromt.coffeee.swf2023.openpromtserver.ipfs.util.JsonToFileWriter.writeJsonToFile;

@Service
public class FileService {

    public MultipartFile convertJsonToMultipartfile(Object request){
        String filePath = "temp.json";
        File file = null;

        try {
            file = writeJsonToFile(request, filePath);
            return convertFileToMultipartFile(file);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            file.delete();
        }
    }
}