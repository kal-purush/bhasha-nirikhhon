package com.roomfit.be.global.initializer;

import com.roomfit.be.chat.domain.ChatRoom;
import com.roomfit.be.chat.infrastructure.ChatRoomRepository;
import com.roomfit.be.user.domain.User;
import com.roomfit.be.user.infrastructure.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class GlobalInitializer implements ApplicationRunner {
    private final UserRepository userRepository;
    private final ChatRoomRepository chatRoomRepository;
    @Override
    public void run(ApplicationArguments args) throws Exception {

        User user1 = User.createUser("nick1","email@email.com","password");
        User user2 = User.createUser("nick2","email2@email.com","password");
        userRepository.save(user1);
        userRepository.save(user2);
        ChatRoom room = ChatRoom.createPrivateRoom("1");
        chatRoomRepository.save(room);

    }
}