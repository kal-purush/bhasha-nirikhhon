package cooper.command;

import cooper.exceptions.InvalidAccessException;
import cooper.exceptions.InvalidForumDeleteByNonOwnerException;
import cooper.exceptions.InvalidForumPostIdException;
import cooper.storage.StorageManager;
import cooper.ui.Ui;
import cooper.forum.ForumManager;
import cooper.verification.SignInDetails;
import cooper.verification.UserRole;
import cooper.resources.ResourcesManager;

/**
 * The child class of Command that handles the 'post comment' command specifically.
 */
public class PostCommentCommand extends Command {
    private int postId;
    private String content;

    public PostCommentCommand(int postId, String content) {
        super();
        this.postId = postId;
        this.content = content;
    }

    /**
     * The override function for executing the 'post comment' command
     * It comments on an existing post
     * the command is being accessed by 'employee' and 'admin' level users.
     * @param signInDetails access role
     * @param resourcesManager handles all manager classes and their access rights
     * @param storageManager save to storage
     */
    @Override
    public void execute(SignInDetails signInDetails, 
            ResourcesManager resourcesManager, StorageManager storageManager) throws InvalidAccessException {
        UserRole userRole = signInDetails.getUserRole();
        ForumManager forumManager = resourcesManager.getForumManager(userRole);
        if (forumManager != null) {
            try {
                String username = signInDetails.getUsername();
                String postContent = forumManager.commentPost(username, content, postId);
                Ui.printCommentPostCommand(username, postContent, content);
            } catch (InvalidForumPostIdException e) {
                Ui.printInvalidForumPostIndexError();
            }
        } else {
            Ui.printEmployeeHelp();
            Ui.printGeneralHelp();
            Ui.printAdminHelp();
            throw new InvalidAccessException();
        }
    }
}
