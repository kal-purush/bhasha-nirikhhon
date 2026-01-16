package hello.consumer;

import hello.HelloService;

public class HelloServiceConsumer {
    public static void main(String[] args) {
        final Iterable<HelloService> services = HelloService.getService();
        for (final HelloService helloService : services) {
            System.out.println(helloService.say(" modular world"));
        }
    }
}