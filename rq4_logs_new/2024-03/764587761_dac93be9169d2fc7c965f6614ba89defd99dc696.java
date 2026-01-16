package km.cd.backend.challenge.controller;

import km.cd.backend.challenge.domain.Challenge;
import km.cd.backend.challenge.dto.ChallengeDTO;
import km.cd.backend.challenge.repository.ChallengeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
public class ChallengeController {

    @Autowired
    private ChallengeRepository challengeRepository;

    private final Logger log = LoggerFactory.getLogger(getClass());
    @GetMapping("/challenge/create_form")
    public String newChallengeForm() {
        return "/challenge/create_form";
    }

    @PostMapping("/challenge/create_form")
    public String createChallenge(@ModelAttribute ChallengeDTO challengeDTO, RedirectAttributes redirectAttributes) {
        log.info(challengeDTO.toString());
        Challenge challenge = challengeDTO.toEntity();
        Challenge saved = challengeRepository.save(challenge);
        log.info(saved.toString());
        redirectAttributes.addAttribute("challenge_id", saved.getChallenge_id());
        return "redirect:/challenge/success";
    }

    @GetMapping("/challenge/success")
    public String showSuccessPage(@RequestParam Integer challenge_id, Model model) {
        model.addAttribute("challenge_id", challenge_id);
        return "/challenge/success";
    }


}