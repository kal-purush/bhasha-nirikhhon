/*
 * Copyright (c) 2020.
 * Content created by:
 * - Andrei García Cuadra
 * - Miguel Hernández Cassel
 *
 * For the module PDS, on university Carlos III de Madrid.
 * Do not share, review nor edit any content without implicitly asking permission to it's owners, as you can contact by this email:
 * andreigarciacuadra@gmail.com
 *
 * All rights reserved.
 */

package Transport4Future.TokenManagement.service;

import Transport4Future.TokenManagement.model.skeleton.Hasher;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The type Hash manager.
 */
public class Sha256Hasher implements Hasher {

    /**
     * Sha 256 encode byte [ ].
     *
     * @param dataToSign the data to sign
     * @return the byte [ ]
     * @throws NoSuchAlgorithmException the no such algorithm exception
     */
    public byte[] encode(String dataToSign) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(dataToSign.getBytes(StandardCharsets.UTF_8));
        return md.digest();
    }
    /**
     * Sha 256 encode byte [ ].
     *
     * @param dataToSign the data to sign
     * @return the byte [ ]
     * @throws NoSuchAlgorithmException the no such algorithm exception
     */
    public byte[] decode(String dataToSign) throws NoSuchAlgorithmException {
        //TODO
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(dataToSign.getBytes(StandardCharsets.UTF_8));
        return md.digest();
    }

    /**
     * Gets sha 256 hex.
     *
     * @param sha256 the sha 256
     * @return the sha 256 hex
     */
    public String getHex(byte[] sha256) {
        return String.format("%064x", new BigInteger(1, sha256));
    }
}