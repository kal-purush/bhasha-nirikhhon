package ch.uzh.ifi.access.course.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

public class WebhooksControllerTest {

    private String gitlabPayload = "{\n" +
            "  \"object_kind\": \"push\",\n" +
            "  \"event_name\": \"push\",\n" +
            "  \"before\": \"02ed94097a0636908a4b56ad70a4b9a8e575995d\",\n" +
            "  \"after\": \"67b1027ed1bc67edf03c02b59b3dd3d6c01b8475\",\n" +
            "  \"ref\": \"refs/heads/master\",\n" +
            "  \"checkout_sha\": \"67b1027ed1bc67edf03c02b59b3dd3d6c01b8475\",\n" +
            "  \"message\": null,\n" +
            "  \"user_id\": 260104,\n" +
            "  \"user_name\": \"Alexander Hofmann\",\n" +
            "  \"user_username\": \"alexhofmann\",\n" +
            "  \"user_email\": \"\",\n" +
            "  \"user_avatar\": \"https://secure.gravatar.com/avatar/294ece14a82bcccf2b99661a13a66070?s=80&d=identicon\",\n" +
            "  \"project_id\": 14185099,\n" +
            "  \"project\": {\n" +
            "    \"id\": 14185099,\n" +
            "    \"name\": \"TestPrivateRepo\",\n" +
            "    \"description\": \"\",\n" +
            "    \"web_url\": \"https://gitlab.com/alexhofmann/testprivaterepo\",\n" +
            "    \"avatar_url\": null,\n" +
            "    \"git_ssh_url\": \"git@gitlab.com:alexhofmann/testprivaterepo.git\",\n" +
            "    \"git_http_url\": \"https://gitlab.com/alexhofmann/testprivaterepo.git\",\n" +
            "    \"namespace\": \"Alexander Hofmann\",\n" +
            "    \"visibility_level\": 0,\n" +
            "    \"path_with_namespace\": \"alexhofmann/testprivaterepo\",\n" +
            "    \"default_branch\": \"master\",\n" +
            "    \"ci_config_path\": null,\n" +
            "    \"homepage\": \"https://gitlab.com/alexhofmann/testprivaterepo\",\n" +
            "    \"url\": \"git@gitlab.com:alexhofmann/testprivaterepo.git\",\n" +
            "    \"ssh_url\": \"git@gitlab.com:alexhofmann/testprivaterepo.git\",\n" +
            "    \"http_url\": \"https://gitlab.com/alexhofmann/testprivaterepo.git\"\n" +
            "  },\n" +
            "  \"commits\": [\n" +
            "    {\n" +
            "      \"id\": \"67b1027ed1bc67edf03c02b59b3dd3d6c01b8475\",\n" +
            "      \"message\": \"Add config.json\\n\",\n" +
            "      \"timestamp\": \"2019-09-06T22:27:34Z\",\n" +
            "      \"url\": \"https://gitlab.com/alexhofmann/testprivaterepo/commit/67b1027ed1bc67edf03c02b59b3dd3d6c01b8475\",\n" +
            "      \"author\": {\n" +
            "        \"name\": \"Alexander Hofmann\",\n" +
            "        \"email\": \"alexhofmann@gmail.com\"\n" +
            "      },\n" +
            "      \"added\": [\"config.json\"],\n" +
            "      \"modified\": [],\n" +
            "      \"removed\": []\n" +
            "    },\n" +
            "    {\n" +
            "      \"id\": \"02ed94097a0636908a4b56ad70a4b9a8e575995d\",\n" +
            "      \"message\": \"Initial commit\",\n" +
            "      \"timestamp\": \"2019-09-06T21:35:54Z\",\n" +
            "      \"url\": \"https://gitlab.com/alexhofmann/testprivaterepo/commit/02ed94097a0636908a4b56ad70a4b9a8e575995d\",\n" +
            "      \"author\": {\n" +
            "        \"name\": \"Alexander Hofmann\",\n" +
            "        \"email\": \"alexhofmann@gmail.com\"\n" +
            "      },\n" +
            "      \"added\": [\"README.md\"],\n" +
            "      \"modified\": [],\n" +
            "      \"removed\": []\n" +
            "    }\n" +
            "  ],\n" +
            "  \"total_commits_count\": 2,\n" +
            "  \"push_options\": {},\n" +
            "  \"repository\": {\n" +
            "    \"name\": \"TestPrivateRepo\",\n" +
            "    \"url\": \"git@gitlab.com:alexhofmann/testprivaterepo.git\",\n" +
            "    \"description\": \"\",\n" +
            "    \"homepage\": \"https://gitlab.com/alexhofmann/testprivaterepo\",\n" +
            "    \"git_http_url\": \"https://gitlab.com/alexhofmann/testprivaterepo.git\",\n" +
            "    \"git_ssh_url\": \"git@gitlab.com:alexhofmann/testprivaterepo.git\",\n" +
            "    \"visibility_level\": 0\n" +
            "  }\n" +
            "}\n";

    private String githubPayload = "{\n" +
            "  \"ref\": \"refs/heads/feature/new-admin-auth-group-#326\",\n" +
            "  \"before\": \"0000000000000000000000000000000000000000\",\n" +
            "  \"after\": \"a8e6a62ded8064bf295f659c0cc9dbe3b78ad443\",\n" +
            "  \"repository\": {\n" +
            "    \"id\": 179278769,\n" +
            "    \"node_id\": \"MDEwOlJlcG9zaXRvcnkxNzkyNzg3Njk=\",\n" +
            "    \"name\": \"Mock-Course\",\n" +
            "    \"full_name\": \"mp-access/Mock-Course\",\n" +
            "    \"private\": false,\n" +
            "    \"owner\": {\n" +
            "      \"name\": \"mp-access\",\n" +
            "      \"email\": null,\n" +
            "      \"login\": \"mp-access\",\n" +
            "      \"id\": 48990413,\n" +
            "      \"node_id\": \"MDEyOk9yZ2FuaXphdGlvbjQ4OTkwNDEz\",\n" +
            "      \"avatar_url\": \"https://avatars1.githubusercontent.com/u/48990413?v=4\",\n" +
            "      \"gravatar_id\": \"\",\n" +
            "      \"url\": \"https://api.github.com/users/mp-access\",\n" +
            "      \"html_url\": \"https://github.com/mp-access\",\n" +
            "      \"followers_url\": \"https://api.github.com/users/mp-access/followers\",\n" +
            "      \"following_url\": \"https://api.github.com/users/mp-access/following{/other_user}\",\n" +
            "      \"gists_url\": \"https://api.github.com/users/mp-access/gists{/gist_id}\",\n" +
            "      \"starred_url\": \"https://api.github.com/users/mp-access/starred{/owner}{/repo}\",\n" +
            "      \"subscriptions_url\": \"https://api.github.com/users/mp-access/subscriptions\",\n" +
            "      \"organizations_url\": \"https://api.github.com/users/mp-access/orgs\",\n" +
            "      \"repos_url\": \"https://api.github.com/users/mp-access/repos\",\n" +
            "      \"events_url\": \"https://api.github.com/users/mp-access/events{/privacy}\",\n" +
            "      \"received_events_url\": \"https://api.github.com/users/mp-access/received_events\",\n" +
            "      \"type\": \"Organization\",\n" +
            "      \"site_admin\": false\n" +
            "    },\n" +
            "    \"html_url\": \"https://github.com/mp-access/Mock-Course\",\n" +
            "    \"description\": \"Course file directory structure \",\n" +
            "    \"fork\": false,\n" +
            "    \"url\": \"https://github.com/mp-access/Mock-Course\",\n" +
            "    \"forks_url\": \"https://api.github.com/repos/mp-access/Mock-Course/forks\",\n" +
            "    \"keys_url\": \"https://api.github.com/repos/mp-access/Mock-Course/keys{/key_id}\",\n" +
            "    \"collaborators_url\": \"https://api.github.com/repos/mp-access/Mock-Course/collaborators{/collaborator}\",\n" +
            "    \"teams_url\": \"https://api.github.com/repos/mp-access/Mock-Course/teams\",\n" +
            "    \"hooks_url\": \"https://api.github.com/repos/mp-access/Mock-Course/hooks\",\n" +
            "    \"issue_events_url\": \"https://api.github.com/repos/mp-access/Mock-Course/issues/events{/number}\",\n" +
            "    \"events_url\": \"https://api.github.com/repos/mp-access/Mock-Course/events\",\n" +
            "    \"assignees_url\": \"https://api.github.com/repos/mp-access/Mock-Course/assignees{/user}\",\n" +
            "    \"branches_url\": \"https://api.github.com/repos/mp-access/Mock-Course/branches{/branch}\",\n" +
            "    \"tags_url\": \"https://api.github.com/repos/mp-access/Mock-Course/tags\",\n" +
            "    \"blobs_url\": \"https://api.github.com/repos/mp-access/Mock-Course/git/blobs{/sha}\",\n" +
            "    \"git_tags_url\": \"https://api.github.com/repos/mp-access/Mock-Course/git/tags{/sha}\",\n" +
            "    \"git_refs_url\": \"https://api.github.com/repos/mp-access/Mock-Course/git/refs{/sha}\",\n" +
            "    \"trees_url\": \"https://api.github.com/repos/mp-access/Mock-Course/git/trees{/sha}\",\n" +
            "    \"statuses_url\": \"https://api.github.com/repos/mp-access/Mock-Course/statuses/{sha}\",\n" +
            "    \"languages_url\": \"https://api.github.com/repos/mp-access/Mock-Course/languages\",\n" +
            "    \"stargazers_url\": \"https://api.github.com/repos/mp-access/Mock-Course/stargazers\",\n" +
            "    \"contributors_url\": \"https://api.github.com/repos/mp-access/Mock-Course/contributors\",\n" +
            "    \"subscribers_url\": \"https://api.github.com/repos/mp-access/Mock-Course/subscribers\",\n" +
            "    \"subscription_url\": \"https://api.github.com/repos/mp-access/Mock-Course/subscription\",\n" +
            "    \"commits_url\": \"https://api.github.com/repos/mp-access/Mock-Course/commits{/sha}\",\n" +
            "    \"git_commits_url\": \"https://api.github.com/repos/mp-access/Mock-Course/git/commits{/sha}\",\n" +
            "    \"comments_url\": \"https://api.github.com/repos/mp-access/Mock-Course/comments{/number}\",\n" +
            "    \"issue_comment_url\": \"https://api.github.com/repos/mp-access/Mock-Course/issues/comments{/number}\",\n" +
            "    \"contents_url\": \"https://api.github.com/repos/mp-access/Mock-Course/contents/{+path}\",\n" +
            "    \"compare_url\": \"https://api.github.com/repos/mp-access/Mock-Course/compare/{base}...{head}\",\n" +
            "    \"merges_url\": \"https://api.github.com/repos/mp-access/Mock-Course/merges\",\n" +
            "    \"archive_url\": \"https://api.github.com/repos/mp-access/Mock-Course/{archive_format}{/ref}\",\n" +
            "    \"downloads_url\": \"https://api.github.com/repos/mp-access/Mock-Course/downloads\",\n" +
            "    \"issues_url\": \"https://api.github.com/repos/mp-access/Mock-Course/issues{/number}\",\n" +
            "    \"pulls_url\": \"https://api.github.com/repos/mp-access/Mock-Course/pulls{/number}\",\n" +
            "    \"milestones_url\": \"https://api.github.com/repos/mp-access/Mock-Course/milestones{/number}\",\n" +
            "    \"notifications_url\": \"https://api.github.com/repos/mp-access/Mock-Course/notifications{?since,all,participating}\",\n" +
            "    \"labels_url\": \"https://api.github.com/repos/mp-access/Mock-Course/labels{/name}\",\n" +
            "    \"releases_url\": \"https://api.github.com/repos/mp-access/Mock-Course/releases{/id}\",\n" +
            "    \"deployments_url\": \"https://api.github.com/repos/mp-access/Mock-Course/deployments\",\n" +
            "    \"created_at\": 1554292040,\n" +
            "    \"updated_at\": \"2019-09-28T16:03:29Z\",\n" +
            "    \"pushed_at\": 1570048010,\n" +
            "    \"git_url\": \"git://github.com/mp-access/Mock-Course.git\",\n" +
            "    \"ssh_url\": \"git@github.com:mp-access/Mock-Course.git\",\n" +
            "    \"clone_url\": \"https://github.com/mp-access/Mock-Course.git\",\n" +
            "    \"svn_url\": \"https://github.com/mp-access/Mock-Course\",\n" +
            "    \"homepage\": null,\n" +
            "    \"size\": 988,\n" +
            "    \"stargazers_count\": 0,\n" +
            "    \"watchers_count\": 0,\n" +
            "    \"language\": \"C\",\n" +
            "    \"has_issues\": true,\n" +
            "    \"has_projects\": true,\n" +
            "    \"has_downloads\": true,\n" +
            "    \"has_wiki\": true,\n" +
            "    \"has_pages\": false,\n" +
            "    \"forks_count\": 0,\n" +
            "    \"mirror_url\": null,\n" +
            "    \"archived\": false,\n" +
            "    \"disabled\": false,\n" +
            "    \"open_issues_count\": 0,\n" +
            "    \"license\": null,\n" +
            "    \"forks\": 0,\n" +
            "    \"open_issues\": 0,\n" +
            "    \"watchers\": 0,\n" +
            "    \"default_branch\": \"master\",\n" +
            "    \"stargazers\": 0,\n" +
            "    \"master_branch\": \"master\",\n" +
            "    \"organization\": \"mp-access\"\n" +
            "  },\n" +
            "  \"pusher\": {\n" +
            "    \"name\": \"mech-studi\",\n" +
            "    \"email\": \"32181052+mech-studi@users.noreply.github.com\"\n" +
            "  },\n" +
            "  \"organization\": {\n" +
            "    \"login\": \"mp-access\",\n" +
            "    \"id\": 48990413,\n" +
            "    \"node_id\": \"MDEyOk9yZ2FuaXphdGlvbjQ4OTkwNDEz\",\n" +
            "    \"url\": \"https://api.github.com/orgs/mp-access\",\n" +
            "    \"repos_url\": \"https://api.github.com/orgs/mp-access/repos\",\n" +
            "    \"events_url\": \"https://api.github.com/orgs/mp-access/events\",\n" +
            "    \"hooks_url\": \"https://api.github.com/orgs/mp-access/hooks\",\n" +
            "    \"issues_url\": \"https://api.github.com/orgs/mp-access/issues\",\n" +
            "    \"members_url\": \"https://api.github.com/orgs/mp-access/members{/member}\",\n" +
            "    \"public_members_url\": \"https://api.github.com/orgs/mp-access/public_members{/member}\",\n" +
            "    \"avatar_url\": \"https://avatars1.githubusercontent.com/u/48990413?v=4\",\n" +
            "    \"description\": null\n" +
            "  },\n" +
            "  \"sender\": {\n" +
            "    \"login\": \"mech-studi\",\n" +
            "    \"id\": 32181052,\n" +
            "    \"node_id\": \"MDQ6VXNlcjMyMTgxMDUy\",\n" +
            "    \"avatar_url\": \"https://avatars2.githubusercontent.com/u/32181052?v=4\",\n" +
            "    \"gravatar_id\": \"\",\n" +
            "    \"url\": \"https://api.github.com/users/mech-studi\",\n" +
            "    \"html_url\": \"https://github.com/mech-studi\",\n" +
            "    \"followers_url\": \"https://api.github.com/users/mech-studi/followers\",\n" +
            "    \"following_url\": \"https://api.github.com/users/mech-studi/following{/other_user}\",\n" +
            "    \"gists_url\": \"https://api.github.com/users/mech-studi/gists{/gist_id}\",\n" +
            "    \"starred_url\": \"https://api.github.com/users/mech-studi/starred{/owner}{/repo}\",\n" +
            "    \"subscriptions_url\": \"https://api.github.com/users/mech-studi/subscriptions\",\n" +
            "    \"organizations_url\": \"https://api.github.com/users/mech-studi/orgs\",\n" +
            "    \"repos_url\": \"https://api.github.com/users/mech-studi/repos\",\n" +
            "    \"events_url\": \"https://api.github.com/users/mech-studi/events{/privacy}\",\n" +
            "    \"received_events_url\": \"https://api.github.com/users/mech-studi/received_events\",\n" +
            "    \"type\": \"User\",\n" +
            "    \"site_admin\": false\n" +
            "  },\n" +
            "  \"created\": true,\n" +
            "  \"deleted\": false,\n" +
            "  \"forced\": false,\n" +
            "  \"base_ref\": \"refs/heads/master\",\n" +
            "  \"compare\": \"https://github.com/mp-access/Mock-Course/compare/feature/new-admin-auth-group-#326\",\n" +
            "  \"commits\": [],\n" +
            "  \"head_commit\": {\n" +
            "    \"id\": \"a8e6a62ded8064bf295f659c0cc9dbe3b78ad443\",\n" +
            "    \"tree_id\": \"3e090ad385e4dcca0442b71d6f3fd4c7a40b2a53\",\n" +
            "    \"distinct\": true,\n" +
            "    \"message\": \"Add absolute link to image\",\n" +
            "    \"timestamp\": \"2019-09-28T18:03:21+02:00\",\n" +
            "    \"url\": \"https://github.com/mp-access/Mock-Course/commit/a8e6a62ded8064bf295f659c0cc9dbe3b78ad443\",\n" +
            "    \"author\": {\n" +
            "      \"name\": \"Alexander Hofmann\",\n" +
            "      \"email\": \"alexhofmann@gmail.com\",\n" +
            "      \"username\": \"a-a-hofmann\"\n" +
            "    },\n" +
            "    \"committer\": {\n" +
            "      \"name\": \"Alexander Hofmann\",\n" +
            "      \"email\": \"alexhofmann@gmail.com\",\n" +
            "      \"username\": \"a-a-hofmann\"\n" +
            "    },\n" +
            "    \"added\": [],\n" +
            "    \"removed\": [],\n" +
            "    \"modified\": [\"assignment_01/exercise_01/description.md\"]\n" +
            "  }\n" +
            "}\n";

    @Test
    public void parseGitlabPayload() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        WebhooksController.WebhookPayload payload = new WebhooksController.WebhookPayload(mapper.readTree(gitlabPayload), true);

        JsonNode repository = payload.getRepository();
        Assertions.assertThat(repository).isNotNull();
        Assertions.assertThat(payload.getHtmlUrl()).isEqualTo("https://gitlab.com/alexhofmann/testprivaterepo");
        Assertions.assertThat(payload.getGitUrl()).isEqualTo("https://gitlab.com/alexhofmann/testprivaterepo.git");
        Assertions.assertThat(payload.getSshUrl()).isEqualTo("git@gitlab.com:alexhofmann/testprivaterepo.git");

        Assertions.assertThat(payload.matchesCourseUrl("https://gitlab.com/alexhofmann/testprivaterepo")).isTrue();
        Assertions.assertThat(payload.matchesCourseUrl("https://gitlab.com/alexhofmann/testprivaterepo.git")).isTrue();
        Assertions.assertThat(payload.matchesCourseUrl("git@gitlab.com:alexhofmann/testprivaterepo.git")).isTrue();
    }

    @Test
    public void parseGithubPayload() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        WebhooksController.WebhookPayload payload = new WebhooksController.WebhookPayload(mapper.readTree(githubPayload), false);

        JsonNode repository = payload.getRepository();
        Assertions.assertThat(repository).isNotNull();
        Assertions.assertThat(payload.getHtmlUrl()).isEqualTo("https://github.com/mp-access/Mock-Course");
        Assertions.assertThat(payload.getGitUrl()).isEqualTo("https://github.com/mp-access/Mock-Course.git");
        Assertions.assertThat(payload.getSshUrl()).isEqualTo("git@github.com:mp-access/Mock-Course.git");

        Assertions.assertThat(payload.matchesCourseUrl("https://github.com/mp-access/Mock-Course")).isTrue();
        Assertions.assertThat(payload.matchesCourseUrl("https://github.com/mp-access/Mock-Course.git")).isTrue();
        Assertions.assertThat(payload.matchesCourseUrl("git@github.com:mp-access/Mock-Course.git")).isTrue();
    }
}