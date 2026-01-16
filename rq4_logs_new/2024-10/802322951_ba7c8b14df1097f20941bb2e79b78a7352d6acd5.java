package com.tour.prevel.quest.controller;

import com.tour.prevel.quest.dto.Position;
import com.tour.prevel.quest.dto.QuestResponse;
import com.tour.prevel.quest.service.QuestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/quest")
public class QuestController {

    private final QuestService questService;

    @ResponseStatus(HttpStatus.OK)
    @GetMapping
    public List<QuestResponse> getQuestListByLocation(
            Position position,
            Authentication auth
    ) {
        return questService.getQuestListByLocation(position, auth.getName());
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @PostMapping("{questId}")
    public void startQuest(
            @PathVariable Long questId,
            @RequestBody @Valid StartQuestRequest request,
            Authentication auth
    ) {
        questService.startQuest(questId, request, auth.getName());
    }
}