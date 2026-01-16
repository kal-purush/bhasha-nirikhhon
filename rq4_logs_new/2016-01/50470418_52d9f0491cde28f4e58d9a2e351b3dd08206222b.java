package GitClientLibrary;

import java.io.IOException;
import java.util.List;

import org.kohsuke.github.GHIssue;
import org.kohsuke.github.GHIssueBuilder;
import org.kohsuke.github.GHIssueState;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;



public class GitHubClient {

	private GitHub github;
	private String username;
	private String password;
	
	/**
	 * 
	 * @param uname
	 * @param pword
	 */
	public GitHubClient(String uname, String pword) {

		username = uname;
		password = pword;
	}
	
	/**
	 * 
	 */
	public void gitHubLogin() {

		try {
			github = GitHub.connectUsingPassword(username, password);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param name
	 * @return GHRepository
	 */
	public GHRepository getRepo(String name) {

		try {
			return github.getRepository(name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 
	 * @param repo
	 * @return List<GHIssue>
	 */
	public List<GHIssue> getRepoIssues(GHRepository repo, GHIssueState state) {

		try {
			return repo.getIssues(state);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	
	/**
	 * 
	 * @param repo
	 * @param issueId
	 * @return GHIssue
	 */
	public GHIssue getIssue(GHRepository repo, int issueId) {
		try {
			return repo.getIssue(issueId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 
	 * @param issue
	 */
	public void reOpenIssue(GHIssue issue) {
		try {
			issue.reopen();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param issue
	 */
	public void closeIssue(GHIssue issue) {
		try {
			issue.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param issue
	 * @param title
	 */
	public void setIssueTitle(GHIssue issue, String title) {
		try {
			issue.setTitle(title);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param issue
	 * @param title
	 */
	public void setIssueBody(GHIssue issue, String body) {
		try {
			issue.setBody(body);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param repo
	 * @param title
	 * @param body
	 * @return GHIssue
	 */
	public GHIssue createIssue(GHRepository repo, String title, String body) {

		GHIssueBuilder ghib = repo.createIssue(title);
		ghib.body(body);
		try {
			return ghib.create();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}