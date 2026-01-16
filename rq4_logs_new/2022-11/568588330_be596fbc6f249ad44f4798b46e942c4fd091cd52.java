package calculator.client;

import com.proto.sum.SumRequest;
import com.proto.sum.SumResponse;
import com.proto.sum.SumServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class CalculatorClient {
    private static void doSum(ManagedChannel channel) {
        SumServiceGrpc.SumServiceBlockingStub stub = SumServiceGrpc.newBlockingStub(channel);
        SumResponse response = stub.sum(SumRequest.newBuilder().setFirst(1).setSecond(5).build());
        System.out.println("Result: " + response.getResult());
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Need one argument to work");
            return;
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50052)
                .usePlaintext()
                .build();

        switch (args[0]) {
            case "sum":
                doSum(channel);
                break;
            default:
                System.out.println("Keyword invalid: " + args[0]);
                break;
        }

        System.out.println("Shutting down");
        channel.shutdown();
    }
}