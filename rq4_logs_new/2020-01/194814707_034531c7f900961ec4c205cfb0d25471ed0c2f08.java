package hw15MessageSystem.messagesystem;

import hw15MessageSystem.db.handlers.UserAddRequestHandler;
import hw15MessageSystem.db.handlers.UserAuthRequestHandler;
import hw15MessageSystem.db.handlers.UserListRequestHandler;
import hw15MessageSystem.db.repository.UsersRepository;
import hw15MessageSystem.frontend.FrontendService;
import hw15MessageSystem.frontend.handlers.UserAddResponseHandler;
import hw15MessageSystem.frontend.handlers.UserAuthResponseHandler;
import hw15MessageSystem.frontend.handlers.UserListResponseHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class MessageSystemInit {

    private final  MsClient frontendMsClient;

    private final MsClient databaseMsClient;

    private final MessageSystem messageSystem;

    private final UsersRepository usersRepository;

    private final FrontendService frontendService;

    public MessageSystemInit(@Qualifier("frontendMsClient") MsClient frontendMsClient,
                             @Qualifier("databaseMsClient") MsClient databaseMsClient,
                             MessageSystem messageSystem,
                             UsersRepository usersRepository,
                             FrontendService frontendService) {

        this.frontendMsClient = frontendMsClient;
        this.databaseMsClient = databaseMsClient;
        this.messageSystem = messageSystem;
        this.usersRepository = usersRepository;
        this.frontendService = frontendService;

        init();
    }

    private void init() {
        databaseMsClient.addHandler(MessageType.USER_AUTH, new UserAuthRequestHandler(usersRepository));
        databaseMsClient.addHandler(MessageType.USER_LIST, new UserListRequestHandler(usersRepository));
        databaseMsClient.addHandler(MessageType.USER_ADD, new UserAddRequestHandler(usersRepository));
        messageSystem.addClient(databaseMsClient);

        frontendMsClient.addHandler(MessageType.USER_AUTH, new UserAuthResponseHandler(frontendService));
        frontendMsClient.addHandler(MessageType.USER_LIST, new UserListResponseHandler(frontendService));
        frontendMsClient.addHandler(MessageType.USER_ADD, new UserAddResponseHandler(frontendService));
        messageSystem.addClient(frontendMsClient);
    }
}