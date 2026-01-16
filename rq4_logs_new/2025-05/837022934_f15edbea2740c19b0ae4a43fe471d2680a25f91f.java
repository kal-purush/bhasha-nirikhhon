package leets.weeth.global.sas.application.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import leets.weeth.domain.user.domain.entity.SecurityUser;
import leets.weeth.global.sas.domain.entity.ClaimsHolder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.oauth2.server.authorization.jackson2.OAuth2AuthorizationServerJackson2Module;

@ReadingConverter
public class BytesToClaimsHolderConverter implements Converter<byte[], ClaimsHolder> {

    private final Jackson2JsonRedisSerializer<ClaimsHolder> serializer;

    public BytesToClaimsHolderConverter() {
        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.registerModules(SecurityJackson2Modules.getModules(BytesToClaimsHolderConverter.class.getClassLoader()));
        objectMapper.registerModule(new OAuth2AuthorizationServerJackson2Module());

        objectMapper.addMixIn(ClaimsHolder.class, ClaimsHolderMixin.class);
        objectMapper.addMixIn(SecurityUser.class, SecurityUserMixin.class);
        objectMapper.addMixIn(Long.class, SecurityUserMixin.class);

        this.serializer = new Jackson2JsonRedisSerializer<>(objectMapper, ClaimsHolder.class);
    }

    @Override
    public ClaimsHolder convert(byte[] value) {
        return this.serializer.deserialize(value);
    }

}