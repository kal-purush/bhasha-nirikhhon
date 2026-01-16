package com.itis.android.lessontwo.repository;

import static com.itis.android.lessontwo.utils.Constants.PAGE_SIZE;
import static com.itis.android.lessontwo.utils.Constants.ZERO_OFFSET;
import static org.junit.Assert.assertEquals;

import android.support.test.runner.AndroidJUnit4;
import com.itis.android.lessontwo.api.ApiFactory;
import com.itis.android.lessontwo.api.CreatorsService;
import com.itis.android.lessontwo.model.creator.Creator;
import com.itis.android.lessontwo.model.creator.CreatorResponse;
import com.itis.android.lessontwo.model.creator.CreatorResponseData;
import io.reactivex.Single;
import io.reactivex.observers.TestObserver;
import io.realm.Realm;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.*;
import org.junit.runner.*;
import retrofit2.http.Path;
import retrofit2.http.Query;

@RunWith(AndroidJUnit4.class)
public class CreatorRepositoryTest {

    private static final Long ID = 59539L;

    private CreatorRepository repository;

    @Before
    public void setUp() throws Exception {
        repository = new CreatorRepositoryImpl();
    }

    @After
    public void tearDown() throws Exception {
        Realm.getDefaultInstance()
                .executeTransaction(realm -> realm.delete(Creator.class));
    }

    @Test
    public void creatorIdSuccess() throws Exception {
        ApiFactory.setCreatorsService(new TestService());
        Creator creator = repository.creator(59539L).blockingGet();
        assertEquals(ID, creator.getId());
    }

    @Test
    public void creatorIdError() throws Exception {
        ApiFactory.setCreatorsService(new TestService());
        TestObserver<Creator> testObserver = new TestObserver<>();
        repository.creator(123L).toObservable().subscribe(testObserver);
        testObserver.await().assertFailure(Throwable.class);
    }

    @Test
    public void creatorIdMockSuccess() throws Exception {
        TestObserver<Creator> testObserver = new TestObserver<>();

        repository.creator(59539L).toObservable().subscribe(testObserver);

        testObserver.await().assertNoErrors();
        testObserver.await().assertValueCount(1);
        assertEquals(ID, testObserver.await().values().get(0).getId());
    }

    @Test
    public void creatorIdMockError() throws Exception {
        TestObserver<Creator> testObserver = new TestObserver<>();

        repository.creator(6455621L).toObservable().subscribe(testObserver);

        testObserver.await().assertFailure(Throwable.class);
    }

    @Test
    public void testCreatorNotSaved() throws Exception {
        repository.creator(59539L).subscribe();

        int savedCount = Realm.getDefaultInstance()
                .where(Creator.class)
                .findAll()
                .size();
        assertEquals(0, savedCount);
    }

    @Test
    public void creatorListMockSuccess() throws Exception {
        TestObserver<List<Creator>> testObserver = new TestObserver<>();
        repository.creatorsTest(ZERO_OFFSET, PAGE_SIZE)
                .toObservable()
                .subscribe(testObserver);

        testObserver.await().assertNoErrors();
        testObserver.await().assertValueCount(20);
    }

    @Test
    public void testCreatorListSaved() throws Exception {
        repository.creatorsTest(ZERO_OFFSET, PAGE_SIZE).subscribe(
                creators -> {
                    int savedCount = Realm.getDefaultInstance()
                            .where(Creator.class)
                            .findAll()
                            .size();
                    assertEquals(20, savedCount);
                }
        );
    }

    @Test
    public void testRepositoriesRestoredFromCache() throws Exception {
        repository.creators(ZERO_OFFSET, PAGE_SIZE).subscribe();

        //force error for loading

        TestObserver<List<Creator>> testObserver = new TestObserver<>();
        repository.creators(ZERO_OFFSET, PAGE_SIZE)
                .toObservable()
                .subscribe(testObserver);

        testObserver.await().assertNoErrors();
        testObserver.await().assertValueCount(20);
    }

    private class TestService implements CreatorsService {

        @Override
        public Single<CreatorResponse> creators(Long offset, Long limit) {
            CreatorResponse response = new CreatorResponse();
            CreatorResponseData data = new CreatorResponseData();
            List<Creator> creatorList = new ArrayList<>();
            for(int i = 0; i < 20; i++){
                Creator creator = new Creator();
                creator.setId((long) i);
                creatorList.add(creator);
            }
            data.setResults(creatorList);
            response.setData(data);
            return Single.just(response);
        }

        @Override
        public Single<CreatorResponse> creatorsTest(Long offset, Long limit) {
            CreatorResponse response = new CreatorResponse();
            CreatorResponseData data = new CreatorResponseData();
            List<Creator> creatorList = new ArrayList<>();
            for(int i = 0; i < 20; i++){
                Creator creator = new Creator();
                creator.setId((long) i);
                creatorList.add(creator);
            }
            data.setResults(creatorList);
            response.setData(data);
            return Single.just(response);
        }

        @Override
        public Single<CreatorResponse> creators(Long id) {
            if (Objects.equals(id, ID)) {
                CreatorResponse response = new CreatorResponse();
                CreatorResponseData data = new CreatorResponseData();
                Creator creator = new Creator();
                creator.setId(59539L);
                List<Creator> res = new ArrayList<>(1);
                res.add(creator);
                data.setResults(res);
                response.setData(data);
                return Single.just(response);
            } else {
                return Single.error(new Throwable());
            }
        }
    }
}