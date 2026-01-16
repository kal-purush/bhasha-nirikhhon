package io.namoosori.rest.service.logic;

import io.namoosori.rest.entity.User;
import io.namoosori.rest.service.UserService;
import io.namoosori.rest.store.UserStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
@RequiredArgsConstructor
public class UserServiceLogic implements UserService {
    private final UserStore userStore;
    /**
     * UserStore DI 방법
     *  - @Autowired 추가
     *  - 생성자 주입
     *  - lombok annotation 이용 : @RequiredArgsConstructor
     *    private final UserStore userStore => 생성자를 통하여 반드시 초기화 해야 한다.
     * */
//    @Autowired
//    private UserStore userStore;
//
//    public UserServiceLogic(UserStore userStore) {
//        this.userStore = userStore;
//    }
    @Override
    public String register(User newUser) {
        userStore.create(newUser);
        return newUser.getId();
    }

    @Override
    public void modify(User newUser) {
        userStore.update(newUser);
    }

    @Override
    public void remove(String id) {
        userStore.delete(id);
    }

    @Override
    public User find(String id) {
        return userStore.retrieve(id);
    }

    @Override
    public List<User> findAll() {
        return userStore.retrieveAll();
    }
}