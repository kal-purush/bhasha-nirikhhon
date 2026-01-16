package de.sage.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.*;
import java.util.logging.Logger;

/**
 * Update checker for github repositories
 *
 * @author SageSphinx63920
 */
public class UpdateChecker {

    private final @NotNull String repoName;
    private final @NotNull Version version;
    private final @NotNull URI uri;
    private final @Nullable Logger logger;
    private final boolean autoNotify;
    private final @NotNull OkHttpClient client;

    private @Nullable String token;

    private @Nullable Version latestVersion;
    private boolean updateAvailable = false;
    private String updateMessage;

    /**
     * Creates the update checker object used for checking if there is a release with a newer version on <strong>ONLY public github repositories</strong>. Use the github api checker for private repositories
     *
     * @param author     author of the repo
     * @param name       name of the repo
     * @param version    currently used version
     * @param autoNotify <i>recommended</i> automatically sends a message if there is an update available with the logger. <i>manually doing this could create problems with the async http call</i>
     * @param logger     logger to send the update message
     */
    public UpdateChecker(@NotNull String author, @NotNull String name, @NotNull String version, boolean autoNotify, @Nullable Logger logger) {
        this.repoName = name;
        this.autoNotify = autoNotify;
        this.version = new Version(removePrefix(version));
        this.logger = logger;

        if (autoNotify && logger == null) {
            throw new NullPointerException("No logger provided with autoNotify set to true. Please provide logger!");
        }

        client = new OkHttpClient().newBuilder().build();

        try {
            this.uri = new URI("https://github.com/" + author + "/" + repoName + "/releases/latest");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the update checker object used for checking if there is a release with a newer version on <strong>public or private github repositories using the github api </strong>
     *
     * @param author     author of the repo
     * @param name       name of the repo
     * @param version    currently used version
     * @param autoNotify <i>recommended</i> automatically sends a message if there is an update available with the logger. <i>manually doing this could create problems with the async http call</i>
     * @param logger     logger to send the update message
     * @param token      github access token with access to the repository's releases
     */
    public UpdateChecker(@NotNull String author, @NotNull String name, @NotNull String version, boolean autoNotify, @Nullable Logger logger, @NotNull String token) {
        this.repoName = name;
        this.version = new Version(removePrefix(version));
        this.token = token;
        this.logger = logger;
        this.autoNotify = autoNotify;

        if (autoNotify && logger == null) {
            throw new NullPointerException("No logger provided with autoNotify set to true. Please provide logger!");
        }

        client = new OkHttpClient().newBuilder().build();

        try {
            this.uri = new URI("https://api.github.com/repos/" + author + "/" + repoName + "/releases/latest");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks is there is an update. This runs async http requests, so it could take some time to update the status.
     * Use #notifyStatus or #getUpdateStatus to do any more actions
     *
     * @throws IOException throws if there is a problem with okhttp
     */
    public void check() throws IOException {

        Request request = new Request.Builder().url(uri.toURL()).build();


        if (token != null) {

            request.headers().newBuilder()
                    .add("Accept", "application/vnd.github+json")
                    .add("Authorization", "Bearer " + token)
                    .add("X-GitHub-Api-Version", "2022-11-28").build();


            client.newCall(request).enqueue(new GithubAPICallback(version));
        } else
            client.newCall(request).enqueue(new GithubPublicCallback(version));
    }

    public void notifyStatus() {
        if (logger != null) {
            if (latestVersion != null) {
                if (updateAvailable) {
                    if (updateMessage == null) {
                        logger.info("There is a newer version of " + repoName + " (" + latestVersion.get() + ")! Current version: " + version.get());
                    } else {
                        String message = updateMessage.replace("@name", repoName)
                                .replace("@latestVersion", latestVersion.get())
                                .replace("@currentVersion", version.get());

                        logger.info(message);
                    }
                }

            } else
                throw new NullPointerException("There is no version to compare to! Probably do to no check done before.");
        } else
            throw new NullPointerException("There is no logger provided!");
    }

    /**
     * Compares the versions and sets the update status
     *
     * @param using  version that is currently used
     * @param latest latest available version
     */
    private void compareVersions(@NotNull Version using, @NotNull Version latest) {
        this.latestVersion = latest;

        switch (using.compareTo(latest)) {
            case 1, 0 -> updateAvailable = false;
            case -1 -> updateAvailable = true;
            default -> throw new RuntimeException("There is an unknown api error!");
        }

        if (autoNotify)
            notifyStatus();
    }

    /**
     * Removes the version prefix
     *
     * @param version the original version string
     * @return the version string without prefix
     */
    @Contract(pure = true)
    private static @NotNull String removePrefix(@NotNull String version) {
        return version.replaceFirst("^v", "");
    }

    /**
     * Callback used for public github repos
     */
    private class GithubPublicCallback implements Callback {
        private final Version usingVersion;

        /**
         * Creates a callback
         *
         * @param version the current used version
         */
        public GithubPublicCallback(Version version) {
            this.usingVersion = version;
        }

        @Override
        public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
            if (response.code() == 404)
                throw new ConnectException("Could not connect to this repo. This could be due to the repository being private, if so try using the other version method with the github api, or it does not exists.");

            String latestURL = response.headers().get("Location");

            if (latestURL == null) {
                throw new IOException("No version found!");
            }

            String[] urlParts = latestURL.split("/");
            Version latest = new Version(removePrefix(urlParts[urlParts.length - 1]));

            compareVersions(usingVersion, latest);
        }

        @Override
        public void onFailure(@NotNull Call call, @NotNull IOException ex) {
            throw new RuntimeException("There was an error during the execution of the request", ex);
        }
    }


    private class GithubAPICallback implements Callback {
        private final Version usingVersion;

        /**
         * Creates a callback
         *
         * @param version the current used version
         */
        public GithubAPICallback(Version version) {
            this.usingVersion = version;
        }

        @Override
        public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
            if (response.code() != 200 || response.body() == null)
                throw new ConnectException("Could not get data from this repo. This could be due to the repository not existing or a wrong token with no read permission on the releases.");

            String responseBody = response.body().string();
            JsonObject bodyJson = (JsonObject) JsonParser.parseString(responseBody);

            String[] tagParts = bodyJson.get("tag_name").getAsString().split("/");
            boolean preRelease = bodyJson.get("prerelease").getAsBoolean();
            Version latest = new Version(removePrefix(tagParts[tagParts.length - 1]), preRelease);

            compareVersions(usingVersion, latest);
        }

        @Override
        public void onFailure(@NotNull Call call, @NotNull IOException ex) {
            throw new RuntimeException("There was an error during the execution of the request", ex);
        }
    }

    /**
     * Sets an own update message that gets send if there is an update available.
     * Placeholder:
     * \@name = repository name
     * \@latestVersion = latest version string
     * \@currentVersion = current version string
     *
     * @param message message that gets send if null uses the default message
     */
    public void setUpdateMessage(@Nullable String message) {
        this.updateMessage = message;
    }

    /**
     * Gets if there is an update available
     *
     * @return true if there is an update available
     */
    public boolean getUpdateStatus() {
        return updateAvailable;
    }

    /**
     * Returns the latest version string
     *
     * @return returns null if there is no latest version probably due to no check done before, else the entire version string
     */
    @Nullable
    public String getLatestVersion() {
        if (latestVersion != null)
            return latestVersion.get();
        else
            return null;
    }
}