package com.searchvids.service;

import com.searchvids.exception.ResourceNotFoundException;
import com.searchvids.model.User;
import com.searchvids.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Service
public class ImageServiceImplementation implements ImageService{

    private static final String USERNAME = "testusername";
    private static final String EMAIL = "test@email.com";
    private static final String PASSWORD = "testpassword";

    private static final Logger LOGGER = LoggerFactory.getLogger(ImageServiceImplementation.class);

    private UserRepository repository;

    public ImageServiceImplementation(UserRepository repository) {
        this.repository = repository;
    }

    @Override
    public void uploadImageFile(Long userId, MultipartFile file) {

        try {
            User user = repository.findById(userId)
                    .orElseThrow(() -> new ResourceNotFoundException("User", "id", userId));

            Byte[] bytes = new Byte[file.getBytes().length];

            int i = 0;

            for (byte b : file.getBytes()) {
                bytes[i++] = b;
            }

            user.setImage(bytes);
            repository.save(user);
        } catch (IOException io) {
            LOGGER.info(io.getMessage());
        }

    }
}