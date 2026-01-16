package np.com.naxa.factsnepal.feed.list;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import np.com.naxa.factsnepal.feed.Fact;
import np.com.naxa.factsnepal.feed.FactsLocalSource;
import np.com.naxa.factsnepal.network.NetworkApiClient;
import np.com.naxa.factsnepal.network.NetworkApiInterface;
import np.com.naxa.factsnepal.network.facts.FactsResponse;

class FactsRemoteSource {

    private static FactsRemoteSource INSTANCE = null;

    static FactsRemoteSource getINSTANCE() {
        if (INSTANCE == null) {
            INSTANCE = new FactsRemoteSource();
        }
        return INSTANCE;
    }

    private Observable<List<Fact>> getFactsByCategoryId(ArrayList<Integer> categoryIds) {
        return NetworkApiClient.getAPIClient().create(NetworkApiInterface.class)
                .getFactsDetailsResponse(categoryIds)
                .subscribeOn(Schedulers.io())
                .map((Function<List<FactsResponse>, List<Fact>>) factsResponses -> {
                    ArrayList<Fact> facts = new ArrayList<>();
                    for (FactsResponse factsResponse : factsResponses) {
                        facts.addAll(factsResponse.getHome());
                    }
                    return facts;
                }).map(facts -> {
                    FactsLocalSource.getINSTANCE().save(facts);
                    return facts;
                });
    }

    Observable<List<Fact>> getAllFacts() {
        return getFactsByCategoryId(null);
    }
}