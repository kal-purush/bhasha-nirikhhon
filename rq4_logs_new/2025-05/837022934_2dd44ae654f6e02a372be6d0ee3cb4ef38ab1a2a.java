package leets.weeth.global.sas.domain.entity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.redis.core.index.Indexed;

import java.time.Instant;

@Getter
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractToken {

    @Indexed
    private String tokenValue;

    private Instant issuedAt;

    private Instant expiresAt;

    private boolean invalidated;

}