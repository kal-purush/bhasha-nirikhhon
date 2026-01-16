/*
 * package org.matgyeojo.controller;
 * 
 * import javax.validation.Valid;
 * 
 * import org.matgyeojo.jwt.TokenProvider; import
 * org.springframework.http.HttpHeaders; import
 * org.springframework.http.HttpStatus; import
 * org.springframework.http.ResponseEntity; import
 * org.springframework.security.authentication.
 * UsernamePasswordAuthenticationToken; import
 * org.springframework.security.config.annotation.authentication.builders.
 * AuthenticationManagerBuilder; import
 * org.springframework.security.core.Authentication; import
 * org.springframework.security.core.context.SecurityContextHolder; import
 * org.springframework.web.bind.annotation.PostMapping; import
 * org.springframework.web.bind.annotation.RequestBody; import
 * org.springframework.web.bind.annotation.RestController;
 * 
 * import lombok.RequiredArgsConstructor;
 * 
 * @RestController
 * 
 * @RequiredArgsConstructor public class AuthController {
 * 
 * private final TokenProvider tokenProvider; private final
 * AuthenticationManagerBuilder authenticationManagerBuilder;
 * 
 * @PostMapping("/authenticate") public ResponseEntity<TokenDTO>
 * authorize(@Valid @RequestBody OperatorDTO operatorDTO) {
 * UsernamePasswordAuthenticationToken authenticationToken = new
 * UsernamePasswordAuthenticationToken(operatorDTO.getLoginId(),
 * operatorDTO.getOprrPswd());
 * 
 * //authenticationManagerBuilder가 호출되면서 CustomUserDetailService가 로드됨.
 * Authentication authentication =
 * authenticationManagerBuilder.getObject().authenticate(authenticationToken);
 * 
 * SecurityContextHolder.getContext().setAuthentication(authentication); String
 * jwt = tokenProvider.createToken(authentication);
 * 
 * HttpHeaders httpHeaders = new HttpHeaders();
 * httpHeaders.add(JwtFilter.AUTHORIZATION_HEADER, "Bearer " + jwt);
 * 
 * return new ResponseEntity<>(new TokenDTO(jwt), httpHeaders, HttpStatus.OK); }
 * }
 */