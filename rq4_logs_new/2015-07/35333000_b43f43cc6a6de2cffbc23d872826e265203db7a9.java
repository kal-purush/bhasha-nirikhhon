package pl.btcgrouppl.btc.backend.commons.test.util.ddd;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pl.btcgrouppl.btc.backend.commons.ddd.models.annotations.EventListenerAnnotation;
import pl.btcgrouppl.btc.backend.commons.ddd.models.exceptions.EventExecutionException;


/**
 * Created by Sebastian Mekal <sebitg@gmail.com> on 01.07.15.
 * <p>
 *     Event consumer throwing {@link pl.btcgrouppl.btc.backend.commons.ddd.models.exceptions.EventExecutionException}
 * </p>
 */
@Data
@NoArgsConstructor
@Component
@Scope("prototype")
public class ExceptionEventConsumer {

    private boolean consumed = false;

    @EventListenerAnnotation
    public void consume(Object event) {
        throw EventExecutionException.create("Is is expected!", EventExecutionException.TYPE.EVENT_DISPATCH_FAIL);
    }
}