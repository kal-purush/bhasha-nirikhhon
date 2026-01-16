package inrating.statistic.controllers;

import inrating.statistic.entity.Post;
import inrating.statistic.service.StatisticService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statistic")
public class StatisticController {
    @Autowired
    private StatisticService statisticService;

    @GetMapping("/{slug}")
    public Post getStatistic(@PathVariable("slug") String slug) {
        return statisticService.getStatistic(slug);
    }
}