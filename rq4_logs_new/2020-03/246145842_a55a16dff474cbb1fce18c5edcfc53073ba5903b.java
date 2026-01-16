package com.v8tix.katix.social.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.v8tix.katix.social.model.Comment;
import com.v8tix.katix.social.model.Post;
import com.v8tix.katix.social.model.User;
import com.v8tix.katix.social.util.MathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.v8tix.katix.social.util.StringHelper.*;
import static org.apache.commons.io.IOUtils.copy;

@Profile("dev")
@Component
public class InitDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitDatabase.class);

    @Bean
    CommandLineRunner init(MongoOperations operations) {
        LOGGER.info(concatStrings(COLON, "Mongo Operations", "Data ingestion started."));
        return args -> {
            final String usersJson = getFileContent("users.json");
            final String postsJson = getFileContent("posts.json");
            final List<User> users = deserializeUsersJson(usersJson);
            final List<Post> posts = deserializePostsJson(postsJson);
            operations.dropCollection(User.class);
            operations.dropCollection(Post.class);
            operations.dropCollection(Comment.class);
            operations.insertAll(users);
            operations.insertAll(samplePosts(operations, posts));
            operations.insertAll(sampleComments(operations));
        };
    }

    private static List<Post> samplePosts(final MongoOperations operations,
                                          final List<Post> posts) {
        final List<User> users = operations.findAll(User.class);
        posts.forEach(post -> {
            final User user = (User) MathHelper.getRandomObject(users);
            final String userId = user.getId();
            post.setUserId(userId);
        });
        return posts;
    }

    private static List<Comment> sampleComments(final MongoOperations operations) {
        final List<User> users = operations.findAll(User.class);
        final List<Post> posts = operations.findAll(Post.class);
        final List<Comment> comments = new ArrayList<>();
        posts.forEach(post -> {
            final User user0 = (User) MathHelper.getRandomObject(users);
            final String user0Id = user0.getId();
            comments.add(new Comment("0", user0Id, post.getId()));
            final User user1 = (User) MathHelper.getRandomObject(users);
            final String user1Id = user1.getId();
            comments.add(new Comment("1", user1Id, post.getId()));
            final User user2 = (User) MathHelper.getRandomObject(users);
            final String user2Id = user2.getId();
            comments.add(new Comment("2", user2Id, post.getId()));
            final User user3 = (User) MathHelper.getRandomObject(users);
            final String user3Id = user3.getId();
            comments.add(new Comment("3", user3Id, post.getId()));
            final User user4 = (User) MathHelper.getRandomObject(users);
            final String user4Id = user4.getId();
            comments.add(new Comment("4", user4Id, post.getId()));
            final User user5 = (User) MathHelper.getRandomObject(users);
            final String user5Id = user5.getId();
            comments.add(new Comment("5", user5Id, post.getId()));
            final User user6 = (User) MathHelper.getRandomObject(users);
            final String user6Id = user6.getId();
            comments.add(new Comment("6", user6Id, post.getId()));
            final User user7 = (User) MathHelper.getRandomObject(users);
            final String user7Id = user7.getId();
            comments.add(new Comment("7", user7Id, post.getId()));
        });
        return comments;
    }

    private static List<User> deserializeUsersJson(final String usersJson) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final User[] usersArray = mapper.readValue(usersJson, User[].class);
            return Arrays.asList(usersArray);
        } catch (IOException e) {
            LOGGER.info(concatStrings(COLON, "Deserialization error", e.getMessage()));
            return Collections.emptyList();
        }
    }

    private static List<Post> deserializePostsJson(final String postsJson) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final Post[] usersArray = mapper.readValue(postsJson, Post[].class);
            return Arrays.asList(usersArray);
        } catch (IOException e) {
            LOGGER.info(concatStrings(COLON, "Deserialization error", e.getMessage()));
            return Collections.emptyList();
        }
    }

    private static String getFileContent(final String path) {
        final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        final StringWriter writer = new StringWriter();
        try {
            final InputStream is = classloader.getResourceAsStream(path);
            assert is != null;
            copy(is, writer);
        } catch (IOException e) {
            LOGGER.info(concatStrings(COLON, "getFileContent", e.getMessage()));
            return EMPTY_STRING;
        }
        return writer.toString();
    }
}