package es.alavpa.examplecode.interactors;

import rx.Subscriber;

/**
 * Created by alavpa on 4/02/17.
 */

public abstract class SimpleSubscriber<T> extends Subscriber<T> {

    public abstract void onBegin();

    public abstract void onSuccess(T result);

    public abstract void onFail(Throwable e);

    @Override
    public void onStart() {
        super.onStart();
        onBegin();
    }

    @Override
    public final void onError(Throwable e) {
        onFail(e);
        onCompleted();
    }

    @Override
    public final void onNext(T t) {
        onSuccess(t);
        onCompleted();
    }
}