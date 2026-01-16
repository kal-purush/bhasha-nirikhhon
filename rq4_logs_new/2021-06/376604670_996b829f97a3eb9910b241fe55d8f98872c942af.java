package ru.alexkm07.techjparser.service;

import org.springframework.stereotype.Service;
import ru.alexkm07.techjparser.model.fildClass.UserEvent;
import ru.alexkm07.techjparser.repository.UserEventRepository;

@Service
public class UserEventService {

    private final UserEventRepository userEventRepository;

    public UserEventService(UserEventRepository userEventRepository) {
        this.userEventRepository = userEventRepository;
    }

    public UserEvent findByName(String name){
        return userEventRepository.findByName(name);
    }

    public UserEvent save(UserEvent userEvent){
        return userEventRepository.saveAndFlush(userEvent);
    }

}