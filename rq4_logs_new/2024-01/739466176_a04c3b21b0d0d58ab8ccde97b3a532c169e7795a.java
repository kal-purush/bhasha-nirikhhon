package com.anghel.music_band_organizer.services.post;

import com.anghel.music_band_organizer.exceptions.post.PostNotFoundException;
import com.anghel.music_band_organizer.models.entities.Post;
import com.anghel.music_band_organizer.repository.PostRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PostServiceValidationImpl implements PostServiceValidation{

    private final PostRepository postRepository;

    public PostServiceValidationImpl(PostRepository postRepository) {
        this.postRepository = postRepository;
    }

//    @Override
//    public void validatePostAlreadyExists(MessageDTO messageDTO) {
//        Post post = postRepository.findMessageByMessageDateAndMessageTime(messageDTO.getMessageDate(), messageDTO.getMessageTime());
//
//        if (post != null) {
//            throw new PostAlreadyExistsException("A post with the date " + messageDTO.getMessageDate() + " and time " + messageDTO.getMessageTime() + " already exists.");
//        }
//    }

    @Override
    public Post getValidPost(Long postId, String methodName) {
        Post post = postRepository.findById(postId)
                .orElseThrow(() -> new PostNotFoundException("Post with id " + postId + " not found."));
        log.info("Message with id {} retrieved. Method: {}.", postId, methodName);

        return post;
    }
}