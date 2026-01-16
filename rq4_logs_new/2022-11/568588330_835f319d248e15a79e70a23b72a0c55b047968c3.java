package calculator.server;

import com.proto.sum.*;
import io.grpc.stub.StreamObserver;

public class CalculatorServiceImpl extends CalculatorServiceGrpc.CalculatorServiceImplBase {
    @Override
    public void sum(SumRequest request, StreamObserver<SumResponse> responseObserver) {
        responseObserver.onNext(SumResponse.newBuilder().setResult(request.getFirst() + request.getSecond()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void primes(PrimesRequest request, StreamObserver<PrimesResponse> responseObserver) {
        int k = 2;
        int N = request.getNumber();
        while (N > 1) {
            if (N % k == 0) {
                responseObserver.onNext(PrimesResponse.newBuilder().setNumber(k).build());
                N = N / k;
            } else {
                ++k;
            }
        }
        responseObserver.onCompleted();
    }
}