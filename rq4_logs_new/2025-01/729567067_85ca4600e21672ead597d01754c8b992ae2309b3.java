package pl.lotto.infrastructure.resultchecker.scheduler;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import pl.lotto.domain.numbergenerator.WinningNumbersGeneratorFacade;
import pl.lotto.domain.resultchecker.ResultCheckerFacade;
import pl.lotto.domain.resultchecker.dto.PlayersDto;

@Component
@AllArgsConstructor
@Log4j2
public class ResultCheckerScheduler {
    private ResultCheckerFacade resultCheckerFacade;
    private WinningNumbersGeneratorFacade winningNumbersGeneratorFacade;

    @Scheduled(cron = "${lotto.result-checker.generateResultsRunOccurrence}")
    public PlayersDto generateResults(){
        log.info("Started generating results");
        if(!winningNumbersGeneratorFacade.areWinningNumbersGeneratedByDate()) {
            log.info("Winning numbers are not generated");
            throw new RuntimeException("Winning numbers are not generated");
        }
        PlayersDto results = resultCheckerFacade.generateResults();
        log.info(String.format("Generated %s results", results.results().size()));
        return results;
    }
}