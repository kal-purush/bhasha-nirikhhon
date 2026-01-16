package by.it_academy.team1.usermessages.service;

import by.it_academy.team1.usermessages.dao.api.IMessageDao;
import by.it_academy.team1.usermessages.dao.api.IUserDao;
import by.it_academy.team1.usermessages.dao.factory.MessageDaoFactory;
import by.it_academy.team1.usermessages.dao.factory.UserDaoFactory;
import by.it_academy.team1.usermessages.service.api.IStatisticsService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsService implements IStatisticsService {
    public static final String TOTAL_USERS_KEY = "total_registration_users";
    public static final String TOTAL_MESSAGES_KEY = "total_messages";
    public static final String TOTAL_ACTIVE_USERS_KEY = "total_active_sessions";
    private final static StatisticsService instance = new StatisticsService();
    private final AtomicInteger sessionCounter;
    private final IUserDao userDao;
    private final IMessageDao messageDao;

    public StatisticsService() {
        this.sessionCounter = new AtomicInteger(0);
        this.userDao = UserDaoFactory.getInstance();
        this.messageDao = MessageDaoFactory.getInstance();
    }
    @Override
    public Map<String, Integer> getStats() {
        Map<String, Integer> stats  = new HashMap<>();
        stats.put(TOTAL_USERS_KEY, this.userDao.getRegistrationUsers().size());
        stats.put(TOTAL_MESSAGES_KEY, this.messageDao.get().size());
        stats.put(TOTAL_ACTIVE_USERS_KEY, this.getSessionCount());
        return stats;
    }

    @Override
    public Integer incSessionCount() {
        return this.sessionCounter.incrementAndGet();
    }

    @Override
    public Integer decSessionCount() {
        return this.sessionCounter.decrementAndGet();
    }

    @Override
    public Integer getSessionCount() {
        return this.sessionCounter.get();
    }


    public static StatisticsService getInstance() {
        return instance;
    }
}