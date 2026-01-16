package tracker.food.good.controller;

import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import tracker.food.good.dto.TwitterPostDto;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

@PropertySource("classpath:config.properties")
@RequestMapping(value = "/social/twitter")
@Controller
public class TwitterController<TwitterApi> {

	private static final String PUBLISH_SUCCESS = "success";



	@Autowired
	HttpSession session;
	
	@Value("${app.config.oauth.twitter.callback}")
	private String callback;
	
	

	
	
	@RequestMapping(value = "/signin", headers = "Accept=application/json",method = {
			org.springframework.web.bind.annotation.RequestMethod.POST })
	public @ResponseBody JSONObject signin(@RequestBody TwitterPostDto twitterPostDto,HttpServletRequest request,
			HttpServletResponse response) throws Exception {
		HttpSession session = request.getSession();
		System.out.println( "TwitterLoginServlet:doGet" );
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		 JSONObject object=new JSONObject();
		
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey("xxxxxxxxxxxxxxxxxxxxxx")
		.setOAuthConsumerSecret("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
		.setOAuthRequestTokenURL("https://api.twitter.com/oauth/request_token")
		.setOAuthAuthorizationURL("https://api.twitter.com/oauth/authorize")
		.setOAuthAccessTokenURL("https://api.twitter.com/oauth/access_token");
	
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		request.getSession().setAttribute("twitter", twitter);
		try {
			TwitterPostDto tpdto=new TwitterPostDto();
			tpdto.setTweetData(twitterPostDto.getTweetData());
			tpdto.setTweetImage(twitterPostDto.getTweetImage());
			session.setAttribute("tweetparams", tpdto);
		StringBuffer callbackURL = request.getRequestURL();
		System.out.println( "TwitterLoginServlet:callbackURL:"+callbackURL );
		
		 
		RequestToken requestToken = twitter.getOAuthRequestToken(callback);
		request.getSession().setAttribute("requestToken", requestToken);
		System.out.println( "requestToken.getAuthenticationURL():"+requestToken.getAuthenticationURL() );
		
		RedirectView redirectView = new RedirectView(requestToken.getAuthenticationURL(), true, true,
				true);
object.put("authenticUrl", requestToken.getAuthenticationURL());
		
return object;
		} catch (TwitterException e) {
			throw new ServletException(e);
			}
		
	}

	@RequestMapping(value = "/callback", method = RequestMethod.GET)
	@ResponseBody
	public String springSocialCallback(
			@RequestParam("oauth_token") String oauthToken,
			@RequestParam("oauth_verifier") String oauthVerifier,
			HttpServletRequest request, HttpServletResponse response)
			throws Exception {
		session = request.getSession();
		TwitterPostDto tpdto=new TwitterPostDto();
		tpdto=(TwitterPostDto) session.getAttribute("tweetparams");
		Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");
		RequestToken requestToken = (RequestToken) request.getSession().getAttribute("requestToken");
		System.out.println( "TwitterCallbackServlet:requestToken:"+requestToken);
		String verifier = request.getParameter("oauth_verifier");
		try {
		twitter.getOAuthAccessToken(requestToken, verifier);
		StatusUpdate statusUpdate = new StatusUpdate(tpdto.getTweetData());
       
       statusUpdate.setMedia(
                
               "Media test"
               , new URL(tpdto.getTweetImage()).openStream());

       Status status = (Status) twitter.updateStatus(statusUpdate);
		request.getSession().removeAttribute("requestToken");
		} catch (TwitterException e) {
		throw new ServletException(e);
		}
		response.sendRedirect(request.getContextPath() + "/socialMediaShare");
		
		return PUBLISH_SUCCESS;
	}
}