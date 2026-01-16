package sky.pro.telegrambotforpets.services;

import com.pengrad.telegrambot.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sky.pro.telegrambotforpets.interfaces.GuestService;
import sky.pro.telegrambotforpets.repositories.GuestRepository;

@Service
@Transactional
public class GuestServiceImpl implements GuestService {
    private final Logger logger = LoggerFactory.getLogger(GuestServiceImpl.class);
    private final GuestRepository guestRepository;

    public GuestServiceImpl(GuestRepository guestRepository) {
        this.guestRepository = guestRepository;
    }

    public String getUserNameOfGuest(Update update) {
        String userName = update.message().from().username();
        if  (userName!=null && !( userName.isEmpty() && userName.isBlank() )) {
            logger.info("выполнился метод getUserNameOfGuest, получили userName");
            return userName;
        }
        logger.info("выполнился метод getUserNameOfGuest, не получили userName");
        return "Гость";
    }


}