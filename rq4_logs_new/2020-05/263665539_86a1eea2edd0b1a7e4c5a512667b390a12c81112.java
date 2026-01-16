package sysjm3.bulbo.Bulbo.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.DeleteMapping;

import sysjm3.bulbo.Bulbo.exceptions.CardNotFoundException;
import sysjm3.bulbo.Bulbo.exceptions.WorkspaceNotFoundException;
import sysjm3.bulbo.Bulbo.model.Card;
import sysjm3.bulbo.Bulbo.repository.CardRepository;
import sysjm3.bulbo.Bulbo.repository.WorkspaceRepository;
import sysjm3.bulbo.Bulbo.model.Workspace;

@RestController
public class JpaController {

    @Autowired
    CardRepository cardRepository;

    @Autowired
    WorkspaceRepository workspaceRepository;

    @GetMapping("/cards")
    public List<Card> allCards() {
        return (List<Card>) cardRepository.findAll();
    }

    @PostMapping("/cards")
    public Card createCard(@RequestBody Card card) {
        return cardRepository.save(card);
    }

    @DeleteMapping("/cards")
    public void deleteCard(@RequestBody Card card) {
            cardRepository.delete(card);
    }

    @GetMapping("/cards/{id}")
    public Card getCardById(@PathVariable Long cardId) {
        return cardRepository.findById(cardId)
                .orElseThrow(() -> new CardNotFoundException(cardId));
    }

    @GetMapping("/cards?by=workspace")
    public Card getCardByWorkspace(@RequestBody Workspace workspace) {
        if (workspace != null) {
            return (Card) cardRepository.findByWorkspace(workspace, Sort.by(Sort.Direction.ASC, "id"));
        } else {
            return null;
        }
    }

    @GetMapping("/workspaces")
    public List<Workspace> allWorkspaces() {
        return (List<Workspace>) workspaceRepository.findAll();
    }

    @PostMapping("/workspaces")
    public Workspace createWorkspace(@RequestBody Workspace workspace) {
        return workspaceRepository.save(workspace);
    }

    @DeleteMapping("/workspace")
    public void deleteWorkspace(@RequestBody Workspace workspace) {
        if (workspace != null) {
            workspaceRepository.delete(workspace);
        }
    }

    @GetMapping("/workspace/{id}")
    public Workspace getWorkspaceById(@PathVariable Long workspaceId) {
        return workspaceRepository.findById(workspaceId)
                .orElseThrow(() -> new WorkspaceNotFoundException(workspaceId));
    }

}