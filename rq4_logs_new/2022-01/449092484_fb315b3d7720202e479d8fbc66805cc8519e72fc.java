package me.wup.gamesurvey.controllers;

import me.wup.gamesurvey.domain.dto.GameDto;
import me.wup.gamesurvey.domain.dto.RecordDto;
import me.wup.gamesurvey.domain.dto.RecordInsertSurveyDto;
import me.wup.gamesurvey.services.GameService;
import me.wup.gamesurvey.services.RecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/records")
public class RecordController {

    @Autowired
    private RecordService recordService;

    @PostMapping()
    public ResponseEntity <RecordDto> findAllGames (@RequestBody RecordInsertSurveyDto dto) {
        RecordDto newDto = recordService.saveGameSurvey(dto);
        return ResponseEntity.ok().body(newDto);
    }
}