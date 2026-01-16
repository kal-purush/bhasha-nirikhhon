package dev.enricosola.porcellino.controller;

import org.springframework.security.authentication.BadCredentialsException;
import dev.enricosola.porcellino.response.portfolio.CreateResponse;
import org.springframework.security.core.userdetails.UserDetails;
import dev.enricosola.porcellino.response.portfolio.EditResponse;
import dev.enricosola.porcellino.response.portfolio.ListResponse;
import org.springframework.web.server.ResponseStatusException;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import dev.enricosola.porcellino.form.portfolio.CreateForm;
import dev.enricosola.porcellino.service.PortfolioService;
import dev.enricosola.porcellino.response.SuccessResponse;
import dev.enricosola.porcellino.form.portfolio.EditForm;
import org.springframework.security.core.Authentication;
import dev.enricosola.porcellino.service.UserService;
import dev.enricosola.porcellino.dto.PortfolioDTO;
import dev.enricosola.porcellino.entity.Portfolio;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import dev.enricosola.porcellino.entity.User;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeToken;
import jakarta.validation.Valid;
import java.lang.reflect.Type;
import java.util.List;

@RestController
@RequestMapping("/api/portfolio")
@CrossOrigin(origins = "*", maxAge = 3600)
public class PortfolioController {
    private final PortfolioService portfolioService;
    private final UserService userService;
    private final ModelMapper modelMapper;

    private User getAuthenticatedUser(Authentication authentication){
        UserDetails userDetails = (UserDetails)authentication.getPrincipal();
        return this.userService.getUserByEmail(userDetails.getUsername())
                .orElseThrow(() -> new BadCredentialsException("Not authenticated."));
    }

    private void setupPortfolio(String portfolioId, Authentication authentication){
        Portfolio portfolio = this.portfolioService.getById(Integer.parseInt(portfolioId));
        User user = this.getAuthenticatedUser(authentication);
        if ( portfolio == null ){
            throw new ResponseStatusException(NOT_FOUND, "No such portfolio found.");
        }
        if ( portfolio.getUser().getId() != user.getId() ){
            throw new ResponseStatusException(FORBIDDEN, "Portfolio access denied.");
        }
    }

    public PortfolioController(PortfolioService portfolioService, UserService userService, ModelMapper modelMapper){
        this.portfolioService = portfolioService;
        this.userService = userService;
        this.modelMapper = modelMapper;
    }

    @GetMapping()
    public ResponseEntity<ListResponse> list(Authentication authentication){
        User user = this.getAuthenticatedUser(authentication);
        Type portfolioListType = new TypeToken<List<PortfolioDTO>>() {}.getType();
        List<PortfolioDTO> portfolioList = this.modelMapper.map(this.portfolioService.getAllByUser(user), portfolioListType);
        return ResponseEntity.ok(new ListResponse(portfolioList));
    }

    @PostMapping("/create")
    public ResponseEntity<CreateResponse> create(@Valid @ModelAttribute CreateForm createForm, Authentication authentication){
        User user = this.getAuthenticatedUser(authentication);
        PortfolioDTO portfolio = this.modelMapper.map(this.portfolioService.createFromForm(user, createForm), PortfolioDTO.class);
        return ResponseEntity.ok(new CreateResponse(portfolio));
    }

    @PatchMapping("/{portfolioId}/edit")
    public ResponseEntity<EditResponse> edit(@Valid @ModelAttribute EditForm editForm, @PathVariable("portfolioId") String portfolioId, Authentication authentication){
        this.setupPortfolio(portfolioId, authentication);
        PortfolioDTO portfolio = this.modelMapper.map(this.portfolioService.updateFromForm(editForm), PortfolioDTO.class);
        return ResponseEntity.ok(new EditResponse(portfolio));
    }

    @DeleteMapping("/{portfolioId}/delete")
    public ResponseEntity<SuccessResponse> delete(@PathVariable("portfolioId") String portfolioId, Authentication authentication){
        this.setupPortfolio(portfolioId, authentication);
        this.portfolioService.delete();
        return ResponseEntity.ok(new SuccessResponse(null));
    }
}