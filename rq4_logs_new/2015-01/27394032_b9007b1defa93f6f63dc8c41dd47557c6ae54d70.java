/*
 * Copyright 2014 Christophe Pollet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.cpollet.shoppist.web.controller;

import com.auth0.jwt.JWTSigner;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.internal.org.apache.commons.codec.binary.Base64;
import net.cpollet.shoppist.web.exceptions.InvalidCredentialsException;
import net.cpollet.shoppist.web.exceptions.InvalidTokenException;
import net.cpollet.shoppist.web.rest.RestResponse;
import net.cpollet.shoppist.web.rest.RestResponseBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Christophe Pollet
 */
@RestController
public class TokenController {
	private final static Logger logger = LoggerFactory.getLogger(TokenController.class);

	private static final String MESSAGE_INVALID_CREDENTIALS = "InvalidCredentials";
	private static final String MESSAGE_INVALID_TOKEN = "InvalidToken";

	@Value("${app.web.token.secretKey}")
	private String secretKey;

	@RequestMapping(value = "/api/v1/token", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public RestResponse create(@RequestParam String username, @RequestParam String password) {
		if (!username.equals("cpollet")) {
			throw new InvalidCredentialsException("Invalid username and/or password.");
		}

		Date now = new Date();

		Map<String, Object> tokenClaims = new HashMap<>();
		tokenClaims.put("sub", username);
		tokenClaims.put("iat", now.getTime());
		tokenClaims.put("exp", new DateTime(now).plusSeconds(3600).toDate().getTime());

		String token = getJWTSigner().sign(tokenClaims);

		logger.info("creating token: {}:{} -> {}", username, password, token);

		return RestResponseBuilder.aRestResponse().withObject(token).build();
	}

	private JWTSigner getJWTSigner() {
		return new JWTSigner(secretKey);
	}

	@RequestMapping(value = "/api/v1/token/{token}", method = RequestMethod.DELETE)
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public RestResponse delete(@PathVariable("token") String token) {
		logger.info("deleting token: {}", token);
		return RestResponseBuilder.aRestResponse().build();
	}

	@RequestMapping(value = "/api/v1/token/{token:.+}", method = RequestMethod.PUT)
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public RestResponse renew(@PathVariable("token") String token) {
		logger.info("renewing token: {}", token);

		try {
			getJWTVerifier().verify(token);
		}
		catch (NoSuchAlgorithmException | InvalidKeyException | IOException | SignatureException e) {
			throw new InvalidTokenException(e.getMessage(), e);
		}

		return RestResponseBuilder.aRestResponse().withObject(token).build();
	}

	private JWTVerifier getJWTVerifier() {
		return new JWTVerifier(getBase64SecretKey());
	}

	private String getBase64SecretKey() {
		return new String(Base64.encodeBase64URLSafe(secretKey.getBytes()));
	}

	@ExceptionHandler({InvalidCredentialsException.class})
	@ResponseStatus(value = HttpStatus.FORBIDDEN)
	@ResponseBody
	public RestResponse credentialsError(HttpServletRequest request, Exception exception) {
		logger.error("InvalidCredentials", exception);
		return RestResponseBuilder.aRestResponse() //
				.withHttpStatus(HttpStatus.FORBIDDEN.value()) //
				.withErrorStatus(MESSAGE_INVALID_CREDENTIALS) //
				.withErrorDescription(exception.getMessage()) //
				.build();
	}

	@ExceptionHandler({InvalidTokenException.class})
	@ResponseStatus(value = HttpStatus.FORBIDDEN)
	@ResponseBody
	public RestResponse tokenError(HttpServletRequest request, Exception exception) {
		logger.error("InvalidToken", exception);
		return RestResponseBuilder.aRestResponse() //
				.withHttpStatus(HttpStatus.FORBIDDEN.value()) //
				.withErrorStatus(MESSAGE_INVALID_TOKEN) //
				.withErrorDescription(exception.getMessage()) //
				.build();
	}
}