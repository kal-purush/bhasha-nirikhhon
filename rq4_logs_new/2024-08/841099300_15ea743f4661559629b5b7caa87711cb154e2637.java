package com.example.hackathon.dataset.application;

import com.example.hackathon.dataset.domain.Data;
import com.example.hackathon.dataset.repository.DataRepository;
import com.opencsv.CSVReader;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class DataService {
    private final DataRepository dataRepository;
    public void saveCsv(MultipartFile file) {
        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            String[] nextLine;
            List<Data> entities = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                Data entity = new Data();
                entity.setMediaType(nextLine[1]);
                entity.setTitleName(nextLine[2]);
                entity.setPlaceName(nextLine[3]);
                entity.setPlaceTypel(nextLine[4]);
                entity.setPlaceOverview(nextLine[5]);
                entity.setOpenTime(nextLine[6]);
                entity.setRestDay(nextLine[8]);
                entity.setAddress(nextLine[9]);
                entity.setMapY(nextLine[10]);
                entity.setMapX(nextLine[11]);
                entity.setTel(nextLine[12]);

                entities.add(entity);
            }

            dataRepository.saveAll(entities);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}