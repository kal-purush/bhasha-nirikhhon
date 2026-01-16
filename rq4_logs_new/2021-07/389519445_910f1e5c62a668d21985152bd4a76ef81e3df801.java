package comsy.was.api;

import comsy.was.data.dto.CharacterDto;
import comsy.was.data.redis.entity.CharacterInfo;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import java.util.List;

public class CharacterRedisTestApi {

    @Getter
    public static class Request extends GenericApi.Request{
        @NotNull
        private Long guid;

        @NotNull
        private Long id;

        public Request(@NotNull String version, @NotNull String token, Long guid, Long id) {
            super(version, token);
            this.guid = guid;
            this.id = id;
        }
    }

    @Getter
    public static class Response extends GenericApi.Response{
        private final CharacterInfo character;

        public Response(@NotNull int status, @NotNull String message, CharacterInfo character) {
            super(status, message);
            this.character = character;
        }
    }
}