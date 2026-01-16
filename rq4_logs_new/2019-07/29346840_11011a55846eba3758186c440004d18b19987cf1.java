package com.chriswk.awesome;

import java.util.Random;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;

public class RandomFlowable {
    public static Flowable<Integer> randomInt(int upperBound) {
        Random random = new Random();
        return Flowable.create(new FlowableOnSubscribe<Integer>() {
            @Override
            public void subscribe(FlowableEmitter<Integer> emitter) throws Exception {
                while (!emitter.isCancelled()) {
                    emitter.onNext(random.nextInt(upperBound));
                }
                emitter.onComplete();
            }
        }, BackpressureStrategy.DROP);
    }

}