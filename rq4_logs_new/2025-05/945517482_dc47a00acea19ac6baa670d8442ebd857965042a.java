package com.gitprism.GitPRism.portfolio_collaborators.controller;

import com.gitprism.GitPRism.portfolio_collaborators.dto.request.AddCollaboratorRequest;
import com.gitprism.GitPRism.portfolio_collaborators.dto.response.PortfolioCollaboratorResponse;
import com.gitprism.GitPRism.portfolio_collaborators.entity.PortfolioCollaborator;
import com.gitprism.GitPRism.portfolio_collaborators.service.PortfolioCollaboratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/portfolios/{portfolioId}/collaborators")
public class PortfolioCollaboratorController {

  private final PortfolioCollaboratorService collaboratorService;

  @PostMapping
  public ResponseEntity<PortfolioCollaboratorResponse> addCollaborator(
      @PathVariable Long portfolioId,
      @RequestBody AddCollaboratorRequest request
  ) {
    if (collaboratorService.isAlreadyCollaborator(portfolioId, request.getUserId())) {
      return ResponseEntity.badRequest().body(
          new PortfolioCollaboratorResponse(
              portfolioId,
              request.getUserId(),
              request.getRole().name(),
              "이미 등록된 협업자입니다."
          )
      );
    }

    PortfolioCollaborator collaborator = collaboratorService.addCollaborator(
        portfolioId, request.getUserId(), request.getRole()
    );

    PortfolioCollaboratorResponse response = new PortfolioCollaboratorResponse(
        collaborator.getPortfolio().getId(),
        collaborator.getUser().getId(),
        collaborator.getRole().name(),
        "협업자가 성공적으로 등록되었습니다."
    );

    return ResponseEntity.ok(response);
  }
}