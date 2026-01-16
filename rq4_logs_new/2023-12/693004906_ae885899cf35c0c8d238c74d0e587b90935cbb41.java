import java.util.Collections;
import java.util.Scanner;
import CRUD.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;



public class Testing {

    User user1 = new User("user1", "password1", 1, 100);
    User user2 = new User("user2", "password2", 2, 1000000);
    User user3 = new User("user3", "password3", 3);

    Post post1 = new Post("Title 1", "Body 1", user1, 200);
    Post post2 = new Post("Title 2", "Body 2", user2, 1000200);
    Post post3 = new Post("Title 3", "Body 3", user1);

    Comment comment1 = new Comment("Comment 1", user2, post1, 300);
    Comment comment2 = new Comment("Comment 2", user2, post1, 1000700);
    Comment comment3 = new Comment("Comment 3", user1, post2);


    PostManager postManager = new PostManager();
    PostManager commentManager = new PostManager();
    UserManager userManager = new UserManager();

    @Test
    public void addUserList() {
        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        assertEquals(user1, userManager.userList.get(0));
        assertEquals(user2, userManager.userList.get(1));
        assertEquals(user3, userManager.userList.get(2));

    }

    @Test
    public void addPostList() {
        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        assertEquals(post1, postManager.postList.get(0));
        assertEquals(post2, postManager.postList.get(1));
        assertEquals(post3, postManager.postList.get(2));
    }

    @Test
    public void addCommentList() {
        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);
        assertEquals(comment1, commentManager.postList.get(0));
        assertEquals(comment2, commentManager.postList.get(1));
        assertEquals(comment3, commentManager.postList.get(2));
    }

    @Test
    public void editLists() {
        postManager.addObject(post1);
        commentManager.addObject(comment1);

        post1.editTitle("New Title", user1);
        comment1.editText("New Comment 1");

        assertEquals(post1.getTitle(), postManager.postList.get(0).getTitle());
        assertEquals("New Title", postManager.postList.get(0).getTitle());
        assertEquals("New Title", post1.getTitle());

        assertEquals(comment1.getBody(), commentManager.postList.get(0).getBody());
        assertEquals("New Comment 1", commentManager.postList.get(0).getBody());
        assertEquals("New Comment 1", comment1.getBody());

    }

    @Test
    public void deleteTesting() {
        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);

        userManager.deleteObject(user1);
        postManager.deleteObject(post1);
        commentManager.deleteObject(comment3);

        Throwable userException = assertThrows(IndexOutOfBoundsException.class, () -> userManager.userList.get(2));
        Throwable postException = assertThrows(IndexOutOfBoundsException.class, () -> postManager.postList.get(2));
        Throwable commentException = assertThrows(IndexOutOfBoundsException.class, () -> commentManager.postList.get(2));
            assertEquals(user2, userManager.userList.get(0));
            assertEquals(user3, userManager.userList.get(1));
            assertEquals("Index 2 out of bounds for length 2", userException.getMessage());
        
            assertEquals(post2, postManager.postList.get(0));
            assertEquals(post3, postManager.postList.get(1));
            assertEquals("Index 2 out of bounds for length 2", postException.getMessage());

            assertEquals(comment1, commentManager.postList.get(0));
            assertEquals(comment2, commentManager.postList.get(1));
            assertEquals("Index 2 out of bounds for length 2", commentException.getMessage());
        
    }

    @Test
    public void upvoteTesting() {
        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);

        userManager.downvote(user2, post3);
        userManager.upvote(user3, post3);
        userManager.upvote(user2, post2);
        userManager.upvote(user3, post2);
        userManager.upvote(user1, post2);
        userManager.downvote(user1, post1);
        userManager.downvote(user2, comment2);
        userManager.upvote(user3, comment1);
        

        assertEquals(-1, userManager.userList.get(0).getKarma());
        assertEquals(3, userManager.userList.get(1).getKarma());
        assertEquals(0, userManager.userList.get(2).getKarma());
        
        assertEquals(-1, postManager.postList.get(0).getKarma());
        assertEquals(3, postManager.postList.get(1).getKarma());
        assertEquals(0, postManager.postList.get(2).getKarma());

        assertEquals(1, commentManager.postList.get(0).getKarma());
        assertEquals(-1, commentManager.postList.get(1).getKarma());
        assertEquals(0, commentManager.postList.get(2).getKarma());
        
    }

    @Test
    public void sortByDateTest() {
        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);


        userManager.downvote(user2, post3);
        userManager.upvote(user3, post3);
        userManager.upvote(user2, post2);
        userManager.upvote(user3, post2);
        userManager.upvote(user1, post2);
        userManager.downvote(user1, post1);
        userManager.downvote(user2, comment2);
        userManager.upvote(user3, comment1);

        userManager.sortByKarma();
        postManager.sortByKarma();
        commentManager.sortByKarma();

        userManager.sortByDate();
        postManager.sortByDate();
        commentManager.sortByDate();

        assertEquals(user1, userManager.userList.get(0));
        assertEquals(user2, userManager.userList.get(1));
        assertEquals(user3, userManager.userList.get(2));

        assertEquals(post1, postManager.postList.get(0));
        assertEquals(post2, postManager.postList.get(1));
        assertEquals(post3, postManager.postList.get(2));

        assertEquals(comment1, commentManager.postList.get(0));
        assertEquals(comment2, commentManager.postList.get(1));
        assertEquals(comment3, commentManager.postList.get(2));        
    }

    @Test
    public void sortByKarmaTest() {
        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);


        userManager.downvote(user2, post3);
        userManager.upvote(user3, post3);
        userManager.upvote(user2, post2);
        userManager.upvote(user3, post2);
        userManager.upvote(user1, post2);
        userManager.downvote(user1, post1);
        userManager.downvote(user2, comment2);
        userManager.upvote(user3, comment1);

        userManager.sortByKarma();
        postManager.sortByKarma();
        commentManager.sortByKarma();

        assertEquals(user2, userManager.userList.get(0));
        assertEquals(user3, userManager.userList.get(1));
        assertEquals(user1, userManager.userList.get(2));

        assertEquals(post2, postManager.postList.get(0));
        assertEquals(post3, postManager.postList.get(1));
        assertEquals(post1, postManager.postList.get(2));

        assertEquals(comment1, commentManager.postList.get(0));
        assertEquals(comment3, commentManager.postList.get(1));
        assertEquals(comment2, commentManager.postList.get(2));        
    }


}