/**
 * integration-common
 *
 * Copyright (C) 2018 Black Duck Software, Inc.
 * http://www.blackducksoftware.com/
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.synopsys.integration.encryption;

import java.nio.charset.Charset;
import java.util.Arrays;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import com.synopsys.integration.exception.EncryptionException;

public class DecryptionService {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final Cipher cipher;

    public DecryptionService(final Cipher cipher) {
        this.cipher = cipher;
    }

    public String decrypt(final String encryptedString) throws EncryptionException {
        if (StringUtils.isBlank(encryptedString)) {
            throw new IllegalArgumentException("Please provide a non-blank encrypted string.");
        }
        final String alteredString = doDecryption(encryptedString, cipher);
        if (alteredString == null) {
            throw new EncryptionException("The decrypted password is null");
        }
        return alteredString;
    }

    private String doDecryption(final String original, final Cipher cipher) throws EncryptionException {
        String decryptedString = null;
        try {
            byte[] bytes = original.getBytes(UTF8);
            final int originalBytesLength = bytes.length;
            // The buffer must be a multiple of 8 or else
            // the Cipher cant handle it
            final int bufferSize = originalBytesLength + (8 - originalBytesLength % 8);
            bytes = Arrays.copyOf(bytes, bufferSize);
            final byte[] buffer = cipher.doFinal(Base64.decodeBase64(bytes));
            decryptedString = new String(buffer, UTF8).trim();
        } catch (final Exception e) {
            throw new EncryptionException(e);
        }
        return decryptedString;
    }

}