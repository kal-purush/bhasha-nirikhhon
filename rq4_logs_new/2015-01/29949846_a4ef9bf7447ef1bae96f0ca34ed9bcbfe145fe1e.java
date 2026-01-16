import java.io.Console;
import java.util.Scanner;

import net.dean.jraw.ApiException;
import net.dean.jraw.RedditClient;
import net.dean.jraw.RedditOAuth2Client;
import net.dean.jraw.http.Credentials;
import net.dean.jraw.http.NetworkException;
import net.dean.jraw.managers.AccountManager;
import net.dean.jraw.managers.AccountManager.SubmissionBuilder;
import net.dean.jraw.models.Listing;
import net.dean.jraw.models.LoggedInAccount;
import net.dean.jraw.models.Submission;
import net.dean.jraw.paginators.SubredditPaginator;


public class Fukkr {
	
	private static String SECRET_KEY = "XJhi3zIKZswt1VufE0tntAyEkdE";
	private static String CLIENT_ID = "3-eHD5rWaZtd0w";
	
	
	private static RedditClient reddit;
	private static Listing<Submission> submissions;

	public static void main(String[] args) throws NetworkException, ApiException {
		reddit = new RedditOAuth2Client("MY-USER-AGENT");
		
		Console console = System.console();
		
	    if (console == null) {
	        System.out.println("Couldn't get Console instance");
	        System.exit(0);
	    }
		
		Scanner in = new Scanner(System.in);
		System.out.print("username: ");
		String username = in.nextLine();

		char passArray[] = console.readPassword("password: ");
		String password = new String(passArray);
		
//		System.out.print("password: ");
//		String password = in.nextLine();

		try {
			System.out.println("Logging in...");
			LoggedInAccount me = reddit.login(Credentials.script(username, password, CLIENT_ID, SECRET_KEY));
			System.out.println("Login Sucess");
		} catch (NetworkException e) {
			System.out.println("Incorrect username or password!");
			//e.printStackTrace();
		} catch (ApiException e) {
			System.out.println("ApiException");
			//e.printStackTrace();
		}
		
		AccountManager manager = new AccountManager(reddit);
		System.out.print("Self-post title: ");
		String title = in.nextLine();
		System.out.print("Post body: ");
		String content = in.nextLine();
		manager.submit(new AccountManager.SubmissionBuilder(content, "jncojeans", title));

		
		in.close();
	}
	/**
	 * Outputs something like 26 posts from a given subreddit to Standard Output. Never called because
	 * it's less interesting than what I ended up doing.
	 * @param subreddit
	 * 			name of the subreddit to display from.
	 */
	public void readSubreddit(String subreddit){
		System.out.print("Enter desired Subreddit: ");
		
		
		SubredditPaginator rlinux = new SubredditPaginator(reddit, subreddit);
		
		int i = 0;
		while(rlinux.hasNext() && i < 15){
			
			submissions = rlinux.next();
			
			for ( Submission sub : submissions){
				System.out.println(i+1 + ": " + sub.getTitle());
				i++;
			}
			
		}
		
		System.out.print("fukkr~");
		
	}

}