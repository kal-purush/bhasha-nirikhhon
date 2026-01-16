package sample;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.context.annotation.Scope;
import sample.CountingActor.Count;
import sample.CountingActor.Get;

/**
 * An actor that can count using an injected CountingService.
 *
 * @note The scope here is prototype since we want to create a new actor
 * instance for use of this bean.
 */
@Named("CountingLambdaActor")
@Scope("prototype")
class CountingLambdaActor extends AbstractLoggingActor {

  @Inject
  private CountingService countingService;

  private int count = 0;

  public CountingLambdaActor() {
    receive(ReceiveBuilder.
      match(Count.class, c -> {
        count = countingService.increment(count);
      }).
      match(Get.class, g -> {
        sender().tell(count, self());
      }).
      matchAny(o -> log().info("received unknown message: " + o)).
      build());
  }

}
