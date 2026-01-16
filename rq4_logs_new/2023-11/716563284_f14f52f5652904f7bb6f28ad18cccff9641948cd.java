package com.botdatamessage.backorder;

import com.botdatamessage.model.Domain;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.hibernate.service.spi.ServiceException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
/** Служба получения информации доменов */
@Slf4j
@Component
public class BackorderUp {
    private OkHttpClient client;

    @Value("${https://backorder.ru/json/?order=desc&expired=1&by=hotness&page=1&items=50}")
    private String daily_domains ;

    public void  /** List<DailyDomain> */ getDailyDomains () throws ServiceException, IOException {
        log.info("getDailyDomains");
        ObjectMapper objectMapper = new ObjectMapper();

        try {Domain[] domains = objectMapper.readValue(new URL(daily_domains), Domain[].class);
            log.info("закачка с сайта");
        } catch (IOException e) {
            // handles IO exceptions
        }
       // List<Domain> list = objectMapper.readValue(stringDomain, new TypeReference<List<Domain>>() { });
       // return list;

    }
//        try (BufferedInputStream inputStream = new BufferedInputStream(new URL(daily_domains).openStream());
//    FileOutputStream fileOS = new FileOutputStream("\\Users\\Игорь\\Downloads\\backorder.json")) {
//        byte data[] = new byte[1024];
//        int byteContent;
//        while ((byteContent = inputStream.read(data, 0, 1024)) != -1) {
//            fileOS.write(data, 0, byteContent);
//        }
//        log.info("закачка с сайта");
//    } catch (IOException e) {
//        // handles IO exceptions
//    }
//        try {
//        Path path = Paths.get("\\Users\\Игорь\\Downloads\\backorder.json");
//        stringDomain = Files.readString(path);
//    } catch (IOException e) {
//        e.printStackTrace();
//    }
}