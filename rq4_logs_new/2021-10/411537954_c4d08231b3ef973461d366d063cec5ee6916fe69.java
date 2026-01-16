package cooper.storage;

import cooper.exceptions.InvalidFileDataException;
import cooper.exceptions.InvalidForumPostIdException;
import cooper.finance.CashFlow;
import cooper.finance.FinanceManager;
import cooper.forum.ForumManager;
import cooper.ui.FileIoUi;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ForumStorage extends Storage {

    public ForumStorage(String filePath) {
        super(filePath);
    }

    public void loadForum(ForumManager forumManager) {
        int currentPost = 0;
        Scanner fileScanner = getScanner(filePath);
        while (fileScanner.hasNext()) {
            String[] post = fileScanner.nextLine().split("[|]");
            if (post[0].equals("P")) {
                currentPost++;
                forumManager.addPost(post[1],post[2]);
            } else {
                assert post[0].equals("C");
                try {
                    forumManager.addComment(post[1],post[2],currentPost - 1);
                } catch (InvalidForumPostIdException e) {
                    // Handle this error properly in V2.1?
                    return;
                }
            }
        }
    }


    public void saveForum(ForumManager forumManager) { 
        try {
            FileWriter fileWriter = new FileWriter(filePath, false);
            System.out.println(filePath);
            var forumPosts = forumManager.getForumPosts();
            for (var post : forumPosts) {
                fileWriter.write("P|" + post.getUsername() + "|" + post.getContent() + "\n");
                for (var comment : post.getComments()) {
                    fileWriter.write("C|" + comment.getUsername() + "|" + comment.getContent() + "\n");
                }
            }

            fileWriter.close();
        } catch (IOException e) {
            FileIoUi.showFileWriteError(e);
            System.exit(1);
        }

    }

}