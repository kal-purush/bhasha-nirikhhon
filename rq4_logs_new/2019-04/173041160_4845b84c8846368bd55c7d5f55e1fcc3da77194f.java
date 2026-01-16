package np.com.naxa.factsnepal.feed.list.resource;

import android.arch.lifecycle.LiveData;

import java.util.List;

import io.reactivex.observers.DisposableObserver;
import np.com.naxa.factsnepal.feed.Fact;
import np.com.naxa.factsnepal.feed.FactsLocalSource;
import np.com.naxa.factsnepal.feed.list.FactsRemoteSource;

public class FactsRepo {

    private static FactsRepo INSTANCE = null;

    public static FactsRepo getINSTANCE() {
        if (INSTANCE == null) {
            INSTANCE = new FactsRepo();
        }
        return INSTANCE;
    }

    private Source[] sources = new Source[]{
            RemoteSource.getINSTANCE(),
            LocalSource.getINSTANCE(),
    };

    public LiveData<List<Fact>> getAllFacts(boolean refreshCache) {
        if (refreshCache) {
            FactsRemoteSource.getINSTANCE().getAllFacts()
                    .subscribe(new DisposableObserver<List<Fact>>() {
                        @Override
                        public void onNext(List<Fact> facts) {

                        }

                        @Override
                        public void onError(Throwable e) {

                        }

                        @Override
                        public void onComplete() {

                        }
                    });
        }
        return FactsLocalSource.getINSTANCE().getAll();
    }

    public LiveData<Fact> getById(String factId, boolean refreshCache) {
        if (refreshCache) {
            //not implemented
        }
        return FactsLocalSource.getINSTANCE().getById(factId);
    }

    public LiveData<List<Fact>> getByCategoryIds(List<Integer> categoryIds, boolean refreshCache) {


        if (refreshCache) {
            FactsRemoteSource.getINSTANCE().getFactsByCategoryId(categoryIds)
                    .subscribe(new DisposableObserver<List<Fact>>() {
                        @Override
                        public void onNext(List<Fact> facts) {

                        }

                        @Override
                        public void onError(Throwable e) {

                        }

                        @Override
                        public void onComplete() {

                        }
                    });
        }
        return FactsLocalSource.getINSTANCE().getByIds(categoryIds);
    }
}