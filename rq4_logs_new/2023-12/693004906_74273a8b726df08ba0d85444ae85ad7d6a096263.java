package com.example;

import com.example.CRUD.*;
import javafx.fxml.FXML;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import javafx.scene.text.Text;
import javafx.scene.text.TextAlignment;
import javafx.stage.Stage;

import java.io.IOException;

public class CommentPageController {

    @FXML
    private Button CreatePostButton;

    @FXML
    private Button DeletePostButton;

    @FXML
    private Button EditPostButton;

    @FXML
    private Button OpenCommentButton;

    @FXML
    private Button OpenUserButton;

    @FXML
    private Button OpenPostButton;

    @FXML
    private Button Refresh;

    @FXML
    private Button SortDateButton;

    @FXML
    private Button SortKarmaButton;

    @FXML
    private Button UpvoteButton;

    @FXML
    private Button DownvoteButton;

    @FXML
    private ListView<String> PostListView;

    @FXML
    private TextField bodyTextField;

    @FXML
    private Button confirmButton;

    @FXML
    private TextField titleTextField;

    private boolean isStarted = false;

    int index;

    User user1 = new User("user1", "password1", 1, 100);
    public User currentUser = user1;
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

    public void initiateList() {

        userManager.addObject(user1);
        userManager.addObject(user2);
        userManager.addObject(user3);

        postManager.addObject(post1);
        postManager.addObject(post2);
        postManager.addObject(post3);

        commentManager.addObject(comment1);
        commentManager.addObject(comment2);
        commentManager.addObject(comment3);
    }

    @FXML
    void CreatePostPressed() {
        Post newPost = new Post("new post", "new body", currentUser);
        postManager.addObject(newPost);
        System.out.println("new post created");
        showPage();
    }

    @FXML
    void DeletePostPressed(MouseEvent event) {
        String selectedPost = PostListView.getSelectionModel().getSelectedItem();
        int index = PostListView.getItems().indexOf(selectedPost);
        postManager.postList.remove(index);
        PostListView.getItems().remove(index);
        currentPosts.getItems().remove(index);
        showPage();
    }

    @FXML
    void EditPostPressed() throws IOException {
        String selectedPost = PostListView.getSelectionModel().getSelectedItem();
        int index = PostListView.getItems().indexOf(selectedPost);
        System.out.println(index);
        if (postManager.postList.get(index).getUser() != currentUser) {
            Pane pane = new Pane();
            Text text = new Text("Not allowed, you are not the correct user");
            text.setTranslateX(100);
            text.setTranslateY(100);
            text.setTextAlignment(TextAlignment.CENTER);
            Scene scene = new Scene(pane, 400, 200);
            pane.getChildren().add(text);
            // Set the stage title and scene, then show the stage
            Stage stage = new Stage();
            stage.setTitle("Not Allowed");
            stage.setScene(scene);
            stage.show();
        } else {
            postManager.postList.get(index).editTitle("new " + postManager.postList.get(index).getTitle(), currentUser);
            postManager.postList.get(index).editBody("new " + postManager.postList.get(index).getBody(), currentUser);
        }
        showPage();
    }

    @FXML
    private void confirmButtonPressed() throws IOException {
        String newTitle = titleTextField.getText();
        String newBody = bodyTextField.getText();

        System.out.println("Pressed");
        App.setRoot("HomePage");

        if (!newTitle.isEmpty()) {
            postManager.postList.get(index).editTitle(newTitle, currentUser);
        }

        if (!newBody.isEmpty()) {
            postManager.postList.get(index).editBody(newBody, currentUser);
        }
    }

    @FXML
    void openUserPressed() {

    }

    @FXML
    void OpenPostPressed() {

    }

    @FXML
    void OpenCommentPressed() {

    }

    @FXML
    void SortDatePressed() {
        postManager.sortByDate();
        showPage();
    }

    @FXML
    void SortKarmaPressed() {
        postManager.sortByKarma();
        showPage();
    }

    @FXML
    void UpvotePressed() {
        String selectedPost = PostListView.getSelectionModel().getSelectedItem();
        int index = PostListView.getItems().indexOf(selectedPost);

        userManager.upvote(currentUser, postManager.postList.get(index));
        showPage();
    }

    @FXML
    void DownvotePressed() {
        String selectedPost = PostListView.getSelectionModel().getSelectedItem();
        int index = PostListView.getItems().indexOf(selectedPost);

        userManager.downvote(currentUser, postManager.postList.get(index));
        showPage();

    }

    private ListView<Post> currentPosts = new ListView<Post>();

    @FXML
    private void showPage() {
        if (isStarted == false) {
            initiateList();
            isStarted = true;
        }

        for (Post post : postManager.postList) {
            System.out.println(post.getTitle());
        }
        if (!PostListView.getItems().isEmpty()) {
            for (Post post : currentPosts.getItems()) {
                PostListView.getItems().remove(0);
            }
        }

        for (Post post : postManager.postList) {
            PostListView.getItems().add("Title: " + post.getTitle() + " Body: " + post.getBody() +
                    " Karma: " + post.getKarma());
        }

        for (Post post : postManager.postList) {
            if (!currentPosts.getItems().contains(post)) {
                currentPosts.getItems().add(post);
            }
        }


    }
}