package io.su0.json;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class TestUtil {

    public static InputStream getResourceAsStream(String resourceName) {
        return TestUtil.class.getClassLoader().getResourceAsStream(resourceName);
    }

    public static InputStream toStream(String string) {
        return new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
    }

    private TestUtil() {
    }
}