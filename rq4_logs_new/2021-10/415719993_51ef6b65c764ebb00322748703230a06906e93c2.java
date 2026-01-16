package actors;

import java.util.Arrays;
import java.util.HashSet;

public class User {
    private String username;
    private String email;
    private String bio;
    private HashSet<String> tags;

    /**
     * Create a user object
     * @param username the user's username
     * @param email the user's email
     * @param bio the user's bio
     * @param tags the user's tags
     */
    public User(String username, String email, String bio, String... tags) {
        this.username = username;
        this.email = email;
        this.bio = bio;
        this.tags = new HashSet<>();
        this.tags.addAll(Arrays.asList(tags));
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getBio() {
        return bio;
    }

    public void setBio(String bio) {
        this.bio = bio;
    }

    public HashSet<String> getTags() {
        return tags;
    }

    /**
     * Add a tag to the user
     * @param tag the tag to add
     */
    public void addTag(String tag) {
        tags.add(tag);
    }

    /**
     * Add several tags to the user
     * @param tags the tags to add
     */
    public void addTags(String... tags) {
        this.tags.addAll(Arrays.asList(tags));
    }

    /**
     * Remove a tag from the user
     * @param tag the tag to remove
     * @return true if successful, false if failed
     */
    public boolean removeTag(String tag) {
        return tags.remove(tag);
    }

    /**
     * Check if a user has a specific tag
     * @param tag the tag to check
     * @return true if the tag exists, false if it doesn't
     */
    public boolean userHasTag(String tag) {
        return tags.contains(tag);
    }
}