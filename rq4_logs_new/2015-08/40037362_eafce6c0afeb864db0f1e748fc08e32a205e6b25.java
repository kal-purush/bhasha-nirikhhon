import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ratpack.exec.ExecController;
import ratpack.exec.Fulfiller;
import ratpack.exec.Result;
import ratpack.exec.internal.DefaultExecController;
import ratpack.func.Action;
import ratpack.http.client.ReceivedResponse;
import ratpack.http.client.RequestSpec;
import ratpack.sse.Event;
import ratpack.sse.ServerSentEventStreamClient;
import ratpack.sse.ServerSentEvents;
import ratpack.stream.Streams;
import ratpack.stream.TransformablePublisher;
import ratpack.test.embed.EmbeddedApp;

import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static java.util.stream.Collectors.joining;
import static ratpack.sse.ServerSentEvents.serverSentEvents;

public class StreamTest {
  @Test
  public void run() throws Exception {
    EmbeddedApp app = EmbeddedApp.fromHandler(context -> {
      // infinite stream of strings
      Iterable<String> infinite =
        () -> Stream.iterate(0, i -> i + 1).limit(5).map(i -> Integer.toString(i)).iterator();
      Publisher<String> stream = Streams.publish(infinite);

      ServerSentEvents events = serverSentEvents(stream, e -> {
          e.id(Objects::toString);
          e.event("counter");
          e.data(i -> "event " + i);
        }
      );

      context.render(events);
    });
    // what you would do to read the events in a single request
    app.test(httpClient -> {
      ReceivedResponse response = httpClient.get();
      assertEquals("text/event-stream;charset=UTF-8", response.getHeaders().get("Content-Type"));

      String expectedOutput = Arrays.asList(0, 1, 2, 3, 4)
                                    .stream()
                                    .map(
                                      i -> "event: counter\ndata: event " + i + "\nid: " + i + "\n")
                                    .collect(joining("\n"))
                              + "\n";

      String text = response.getBody().getText();
      assertEquals(expectedOutput, text);
    });

    URI address = app.getAddress();
    TransformablePublisher<Event<?>> streamResults = request(address);
    CountDownLatch latch = new CountDownLatch(1);
    streamResults.subscribe(new Subscriber<Event<?>>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Event<?> event) {
        System.out.println(event.getData());
      }

      @Override
      public void onError(Throwable throwable) {
        System.err.println(throwable);
      }

      @Override
      public void onComplete() {
        latch.countDown();
      }
    });
    latch.await();
  }

  public static TransformablePublisher<Event<?>> request(URI uri) throws Exception {
    try (ExecController execController = new DefaultExecController(2)) {
      // run the request within the ratpack managed thread context
      StreamAction action = new StreamAction(uri, execController, Action.noop());
      FulfillerImpl<TransformablePublisher<Event<?>>> fulfiller = new FulfillerImpl<>();
      execController.getControl().fork()
                    .start(exec -> action.execute(fulfiller));
      // block until we get a stream event publisher
      return fulfiller.get();
    }
  }


  /**
   * Essentially, we need an action to encapsulate the request, so we can run it as an Action on
   * the Ratpack threadpool. The promise supplied by the client is then used to populate the
   * Fulfiller and return the result back to the request caller
   */
  private static class StreamAction implements Action<Fulfiller<TransformablePublisher<Event<?>>>> {

    private final URI uri;
    private final ServerSentEventStreamClient events;
    private final Action<? super RequestSpec> action;

    public StreamAction(URI uri, ExecController execController,
      Action<? super RequestSpec> setupRequest) {
      this.uri = uri;
      this.events = ServerSentEventStreamClient.sseStreamClient(execController,
        UnpooledByteBufAllocator.DEFAULT);
      this.action = setupRequest;
    }

    @Override
    public void execute(Fulfiller<TransformablePublisher<Event<?>>> promiseFulfiller)
      throws Exception {
      // the stream client runs the execution. The result is async handed back to the fullfiller,
      // which will pass it back to the caller. Its a bit complicated, but seems to be the only
      // way to manage promise passing
      events.request(uri, action)
            .onError(t -> promiseFulfiller.error(t))
            .then(result -> promiseFulfiller.success(result));

    }
  }


  private static class FulfillerImpl<T> implements Fulfiller<T> {

    private CountDownLatch done = new CountDownLatch(1);
    private Result<T> result;

    @Override
    public void error(Throwable throwable) {
      setResult(Result.error(throwable));
    }

    @Override
    public void success(T value) {
      setResult(Result.success(value));
    }

    private void setResult(Result<T> result) {
      this.result = result;
      done.countDown();
    }

    public T get() throws Exception {
      done.await();
      if (result.isSuccess()) {
        return result.getValue();
      }
      Throwable t = result.getThrowable();
      throw t instanceof Exception ? (Exception) t : new Exception(t);
    }
  }
}