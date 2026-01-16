package restserver.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequestMapping("api/test")
@RestController
public class TestRestController {

    @GetMapping("")
    Mono<Map<String, Object>> get() {
        final Instant instant = Instant.now();
        final String date = instant.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        final long epoch = instant.toEpochMilli();
        return Mono.just(new HashMap<>(){
            {
                put("date", date);
                put("epoch", epoch);
            }
        });
    }

}