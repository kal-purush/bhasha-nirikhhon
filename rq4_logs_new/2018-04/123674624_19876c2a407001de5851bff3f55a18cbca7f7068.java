package com.itis.android.lessontwo.repository;

import com.itis.android.lessontwo.api.ApiFactory;
import com.itis.android.lessontwo.api.CharactersService;
import com.itis.android.lessontwo.api.ComicsService;
import com.itis.android.lessontwo.model.character.Character;
import com.itis.android.lessontwo.model.character.CharactersResponse;
import com.itis.android.lessontwo.model.character.CharactersResponseData;
import com.itis.android.lessontwo.model.comics.Comics;
import com.itis.android.lessontwo.model.comics.ComicsResponse;
import com.itis.android.lessontwo.model.comics.ComicsResponseData;
import com.itis.android.lessontwo.model.creator.Creator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.reactivex.Single;
import io.reactivex.observers.TestObserver;
import io.realm.Realm;
import retrofit2.http.Path;
import retrofit2.http.Query;

import static com.itis.android.lessontwo.utils.Constants.DEFAULT_COMICS_SORT;
import static com.itis.android.lessontwo.utils.Constants.PAGE_SIZE;
import static com.itis.android.lessontwo.utils.Constants.ZERO_OFFSET;
import static org.junit.Assert.assertEquals;

public class CharacterRepositoryTest {

    private static final Long ID = 59539L;

    private CharacterRepository repository;

    @Before
    public void setUp() throws Exception {
        repository = new CharacterRepositoryImpl();
    }

    @After
    public void tearDown() throws Exception {
        Realm.getDefaultInstance()
                .executeTransaction(realm -> realm.delete(Character.class));
    }

    @Test
    public void characterIdSuccess() throws Exception {
        ApiFactory.setCharactersService(new TestService());
        Character character = repository.character(59539L).blockingGet();
        assertEquals(ID, character.getId());
    }

    @Test
    public void characterIdError() throws Exception {
        ApiFactory.setCharactersService(new TestService());
        TestObserver<Character> testObserver = new TestObserver<>();
        repository.character(123L).toObservable().subscribe(testObserver);
        testObserver.await().assertFailure(Throwable.class);
    }

    @Test
    public void characterIdMockSuccess() throws Exception {
        TestObserver<Character> testObserver = new TestObserver<>();

        repository.character(59539L).toObservable().subscribe(testObserver);

        testObserver.await().assertNoErrors();
        testObserver.await().assertValueCount(1);
        assertEquals(ID, testObserver.await().values().get(0).getId());
    }

    @Test
    public void characterIdMockError() throws Exception {
        TestObserver<Character> testObserver = new TestObserver<>();

        repository.character(6455621L).toObservable().subscribe(testObserver);

        testObserver.await().assertFailure(Throwable.class);
    }

    @Test
    public void testCharacterNotSaved() throws Exception {
        repository.character(59539L).subscribe();

        int savedCount = Realm.getDefaultInstance()
                .where(Character.class)
                .findAll()
                .size();
        assertEquals(0, savedCount);
    }

    @Test
    public void characterListMockSuccess() throws Exception {
        TestObserver<List<Character>> testObserver = new TestObserver<>();
        repository.characterTest(ZERO_OFFSET, PAGE_SIZE)
                .toObservable()
                .subscribe(testObserver);

        testObserver.await().assertNoErrors();
        testObserver.await().assertValueCount(20);
    }

    @Test
    public void testCharacterListSaved() throws Exception {
        repository.characterTest(ZERO_OFFSET, PAGE_SIZE).subscribe();

        int savedCount = Realm.getDefaultInstance()
                .where(Character.class)
                .findAll()
                .size();
        assertEquals(20, savedCount);
    }

    @Test
    public void testRepositoriesRestoredFromCache() throws Exception {
        repository.characters(ZERO_OFFSET, PAGE_SIZE).subscribe();

        //force error for loading

        TestObserver<List<Character>> testObserver = new TestObserver<>();
        repository.characters(ZERO_OFFSET, PAGE_SIZE)
                .toObservable()
                .subscribe(testObserver);

        testObserver.assertNoErrors();
        testObserver.assertValueCount(20);
    }

    private class TestService implements CharactersService {

        @Override
        public Single<CharactersResponse> characters(@Query("offset") Long offset,
                                                     @Query("limit") Long limit) {
            CharactersResponse response = new CharactersResponse();
            CharactersResponseData data = new CharactersResponseData();
            List<Character> characterList = new ArrayList<>();
            for(int i = 0; i < 20; i++){
                Character character = new Character();
                character.setId((long) i);
                characterList.add(character);
            }
            data.setResults(characterList);
            response.setData(data);
            return Single.just(response);
        }

        @Override
        public Single<CharactersResponse> charactersTest(Long offset, Long limit) {
            CharactersResponse response = new CharactersResponse();
            CharactersResponseData data = new CharactersResponseData();
            List<Character> characterList = new ArrayList<>();
            for(int i = 0; i < 20; i++){
                Character character = new Character();
                character.setId((long) i);
                characterList.add(character);
            }
            data.setResults(characterList);
            response.setData(data);
            return Single.just(response);
        }

        @Override
        public Single<CharactersResponse> character(@Path("comicsId") Long id) {
            if (Objects.equals(id, ID)) {
                CharactersResponse response = new CharactersResponse();
                CharactersResponseData data = new CharactersResponseData();
                Character character = new Character();
                character.setId(59539L);
                List<Character> res = new ArrayList<>(1);
                res.add(character);
                data.setResults(res);
                response.setData(data);
                return Single.just(response);
            } else {
                return Single.error(new Throwable());
            }
        }

        @Override
        public Single<ComicsResponse> comicsByCharacter(Long id) {
            return null;
        }
    }
}