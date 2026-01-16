/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package dk.kb.image;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Hacked unit test as it expects www.kb.dk to be available.
 */
class ProxyHelperTest {

    @Test
    void anyBytes() throws IOException, URISyntaxException {
        try (ByteArrayOutputStream content = new ByteArrayOutputStream()) {
            ProxyHelper.proxy("KB", new URI("https://www.kb.dk/"), new URI("http://example.com/irrelevantfortesting")).write(content);
            assertTrue(content.size() >= 1, "The resulting content should have at least 1 byte");
        }
    }

    @Test
    void contentType() throws IOException, URISyntaxException {
        HttpServletResponse servletResponse = mock(HttpServletResponse.class);

        try (ByteArrayOutputStream content = new ByteArrayOutputStream()) {
            ProxyHelper.proxy("KB",
                              new URI("https://www.kb.dk/"),
                              new URI("http://example.com/irrelevantfortesting"),
                              servletResponse).
                    write(content);
            assertTrue(content.size() >= 1, "The resulting content should have at least 1 byte");
        }

        // We know the response from www.kb.dk starts with text/html
        verify(servletResponse).setContentType(ArgumentMatchers.startsWith("text/html"));
    }
}