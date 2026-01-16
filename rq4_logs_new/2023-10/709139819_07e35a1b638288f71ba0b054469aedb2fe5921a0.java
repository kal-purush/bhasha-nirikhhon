package com.thanksd.server.domain;

import com.thanksd.server.exception.badrequest.InvalidNationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Objects;

@NoArgsConstructor
@AllArgsConstructor
@Getter
public enum Nation {

    KOREA("korea"),
    NETHERLANDS("netherlands");

    private String value;

    public static Nation from(String value) {
        return Arrays.stream(values())
                .filter(it -> Objects.equals(it.value, value))
                .findFirst()
                .orElseThrow(InvalidNationException::new);
    }
}