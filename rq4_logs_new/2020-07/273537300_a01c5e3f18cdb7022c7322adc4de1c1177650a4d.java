// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import com.google.sps.model.AuthenticationVerifier;
import javax.servlet.http.Cookie;
import org.junit.Before;
import org.mockito.Mockito;

/**
 * A base test class for servlets that contains default authentication handling (authentication
 * always passes when using this test base)
 */
public abstract class AuthenticatedServletTestBase extends ServletTestBase {
  protected AuthenticationVerifier authenticationVerifier;

  protected static final boolean AUTHENTICATION_VERIFIED = true;
  protected static final String ID_TOKEN_KEY = "idToken";
  protected static final String ID_TOKEN_VALUE = "sampleId";
  protected static final String ACCESS_TOKEN_KEY = "accessToken";
  protected static final String ACCESS_TOKEN_VALUE = "sampleAccessToken";

  protected static final Cookie sampleIdTokenCookie = new Cookie(ID_TOKEN_KEY, ID_TOKEN_VALUE);
  protected static final Cookie sampleAccessTokenCookie =
      new Cookie(ACCESS_TOKEN_KEY, ACCESS_TOKEN_VALUE);
  protected static final Cookie[] validCookies =
      new Cookie[] {sampleIdTokenCookie, sampleAccessTokenCookie};

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    authenticationVerifier = Mockito.mock(AuthenticationVerifier.class);

    // Authentication will always pass
    Mockito.when(authenticationVerifier.verifyUserToken(ID_TOKEN_VALUE))
        .thenReturn(AUTHENTICATION_VERIFIED);

    Mockito.when(request.getCookies()).thenReturn(validCookies);
  }
}