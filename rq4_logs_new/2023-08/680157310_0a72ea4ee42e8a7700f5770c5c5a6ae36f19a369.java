package com.friska.javaaes;

import com.friska.javaaes.cipher.Cipher;

abstract public class EncryptionType<T extends Cipher<T>> implements Cipher<T> {
    @Override
    public T encrypt() {
        return null;
    }

    @Override
    public T decrypt() {
        return null;
    }
}