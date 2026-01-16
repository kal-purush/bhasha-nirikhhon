package tripagramex.domain.account.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import tripagramex.domain.account.dto.TestDto;
import tripagramex.domain.account.service.AccountService;

@RestController
@RequestMapping("/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @GetMapping
    public String test() {
        return "hello";
    }

    @PostMapping("/testPost")
    public String testPost(@RequestBody TestDto testDto) {
        accountService.testAccountPost(testDto.getEmail(), testDto.getPassword());
        return "success";
    }
}