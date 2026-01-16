package com.green.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;

public final class ClobUtils {

    private ClobUtils() {
        // 인스턴스화 방지
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String clobToString(Clob clob) throws SQLException, IOException {
        if (clob == null) return null;
        StringBuilder sb = new StringBuilder();
        try (Reader reader = clob.getCharacterStream();
             BufferedReader br = new BufferedReader(reader)) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }
}