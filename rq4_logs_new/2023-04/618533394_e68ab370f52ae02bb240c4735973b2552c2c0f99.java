package com.novi.eindopdracht.recipeDiary.recipeDiary.service;

import com.novi.eindopdracht.recipeDiary.recipeDiary.dto.RecipeDiaryDto;
import com.novi.eindopdracht.recipeDiary.recipeDiary.dto.UserDto;
import com.novi.eindopdracht.recipeDiary.recipeDiary.exception.ResourceNotFoundException;
import com.novi.eindopdracht.recipeDiary.recipeDiary.model.RecipeDiary;
import com.novi.eindopdracht.recipeDiary.recipeDiary.model.User;
import com.novi.eindopdracht.recipeDiary.recipeDiary.repository.RecipeDiaryRepository;
import com.novi.eindopdracht.recipeDiary.recipeDiary.repository.UserRepository;
import org.hibernate.Hibernate;
import org.springframework.stereotype.Service;

@Service
public class RecipeDiaryUserService {

    private final RecipeDiaryRepository rdRepos;
    private final UserRepository uRepos;


    public RecipeDiaryUserService(RecipeDiaryRepository rdRepos, UserRepository uRepos) {
        this.rdRepos = rdRepos;
        this.uRepos = uRepos;
    }

    public UserDto getDiaryFromUser(Long userId) {
        User user = uRepos.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User", "id", userId));

        RecipeDiary recipeDiary = user.getRecipeDiary();
        if (recipeDiary != null) {
            Hibernate.initialize(recipeDiary);
        }

        UserDto userDto = new UserDto();
        userDto.setUserId(user.getUserId());
        userDto.setRecipeDiary(recipeDiary);

        return userDto;
    }


    public RecipeDiaryDto linkUserToRecipeDiary(Long userId, Long recipeDiaryId) {

        User user = uRepos.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User", "id", userId));

        RecipeDiary recipeDiary = rdRepos.findById(recipeDiaryId)
                .orElseThrow(()-> new ResourceNotFoundException("RecipeDiary", "id", recipeDiaryId));


        user.setRecipeDiary(recipeDiary);
        recipeDiary.setUser(user);

        RecipeDiary updatedRecipeDiary = rdRepos.save(recipeDiary);
        uRepos.save(user);

        return recipeDiaryUserModelToDto(updatedRecipeDiary);
    }

    public RecipeDiaryDto recipeDiaryUserModelToDto(RecipeDiary recipeDiary){

        RecipeDiaryDto recipeDiaryDto = new RecipeDiaryDto();
        recipeDiaryDto.setRecipeDiaryId(recipeDiary.getRecipeDiaryId());

        return recipeDiaryDto;

    }


}