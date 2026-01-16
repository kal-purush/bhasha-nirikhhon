package yanna.stepanova.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import yanna.stepanova.dto.player.CreatePlayerRequestDto;
import yanna.stepanova.dto.player.PlayerDto;
import yanna.stepanova.service.PlayerService;

import java.util.List;

@RequiredArgsConstructor
@RestController
@RequestMapping("/players")
@Validated
public class PlayerController {
    private final PlayerService playerService;

    @PostMapping
    public PlayerDto createPlayer(@RequestBody @Valid CreatePlayerRequestDto requestDto) {
        return playerService.save(requestDto);
    }

    @GetMapping("/{id}")
    public PlayerDto getPlayerById(@PathVariable @Positive Long id) {
        return playerService.getPlayerById(id);
    }

    @GetMapping
    public List<PlayerDto> getAllPlayers() {
        return playerService.getAll();
    }

    @GetMapping("/by-team")
    public List<PlayerDto> getAllByTeam(@RequestParam Long teamId) {
        return playerService.getAllByTeamId(teamId);
    }

    @PutMapping("/{id}")
    public PlayerDto updatePlayerById(@PathVariable @Positive Long id,
                                      @RequestBody @Valid CreatePlayerRequestDto requestDto) {
        return playerService.updatePlayerById(id, requestDto);
    }

    @DeleteMapping("/{id}")
    public void deletePlayerById(@PathVariable @Positive Long id) {
        playerService.deletePlayerById(id);
    }
}