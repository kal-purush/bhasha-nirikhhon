package jm.controller.view;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ViewController {

    @GetMapping(value = "/")
    public String indexPage() {
        return "index";
    }

    @GetMapping(value = "/news")
    public String newsPage() {
        return "news";
    }

    @GetMapping(value = "/text-me-the-app")
    public String textAppPage() {
        return "text-me-the-app";
    }

    @GetMapping(value = "/portfolios")
    public String portfolioPage() {
        return "portfolios";
    }

    @GetMapping(value = "/how-it-works")
    public String howWorksPage() {
        return "how-it-works";
    }

    @GetMapping(value = "/help")
    public String helpPage() {
        return "help";
    }

    @GetMapping(value = "/sell")
    public String sellPage() {
        return "sell";
    }

    @GetMapping(value = "/reviews")
    public String reviewPage() {
        return "reviews";
    }

    @GetMapping(value = "/privacy")
    public String privacyPage() {
        return "privacy";
    }

    @GetMapping(value = "/terms")
    public String termsPage() {
        return "terms";
    }

    @GetMapping(value = "/jobs")
    public String jobsPage() {
        return "jobs";
    }

    @GetMapping(value = "/press")
    public String pressPage() {
        return "press";
    }
}
