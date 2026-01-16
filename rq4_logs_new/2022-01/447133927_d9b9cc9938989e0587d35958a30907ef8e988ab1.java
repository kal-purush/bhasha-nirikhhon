package com.purbon.kafka.streams.config.auth;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class Credentials {
    private String source;
}