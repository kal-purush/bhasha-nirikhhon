package potatoes.server.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import potatoes.server.utils.annotation.Authorization;
import potatoes.server.utils.jwt.JwtTokenUtil;

@RequiredArgsConstructor
@RestController
public class TokenTestController {

	private final JwtTokenUtil jwtTokenUtil;

	@GetMapping("/token")
	public String exampleToken(@RequestParam(name = "id") String id) {
		return jwtTokenUtil.createToken(id);
	}

	@GetMapping("/token/decode")
	public Long exampleToken2(@Authorization Long id) {
		return id;
	}
	// FIXME 토큰 사용예시용으로 등록한 테스트 컨트롤러이며 추후에 삭제 요망
}