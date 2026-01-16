package yanna.stepanova.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import yanna.stepanova.dto.transfer.TransferDto;
import yanna.stepanova.service.TransferService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/transfer")
@Validated
public class TransferController {
    private final TransferService transferService;

    @PostMapping("/{playerId}/to/{teamId}")
    public TransferDto transferPlayer(@PathVariable Long playerId, @PathVariable Long teamId) {
        return transferService.transferPlayer(playerId, teamId);
    }
}