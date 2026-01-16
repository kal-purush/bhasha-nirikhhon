package demo;

import java.util.Date;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/*TwitterStreamの監視(Investigation)*/
public class Investigation {
	private static final String CONSUMER_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    private static final String CONSUMER_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    private static final String ACCESS_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    private static final String ACCESS_TOKEN_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    
    static class MyStatusListener implements StatusListener {
    	
        public void onStatus(Status status) {
        	String name = status.getUser().getName();//UserName
            String username = "@"+status.getUser().getScreenName();//ScreenName
            String userId = String.valueOf(status.getUser().getId());//UserId
            String tweet = status.getText();//Tweet
            Date createddate = status.getCreatedAt();//CreatedDate
            String timezone = status.getUser().getTimeZone();//TimeZone
            String userlang = status.getUser().getLang();//UserLanguage
            String app = status.getSource();//Application
            String profilegeo = status.getUser().getLocation();//UserProfileLocation
            GeoLocation geo = status.getGeoLocation();//Geolocation
            
            //文字列の中に日本語が含まれている場合のみ出力する
            if (containsNihongo(status.getText()) == true){
            	//ツイートから改行記号を除去（半角スペースに変換）
            	tweet = tweet.replaceAll("\r\n"," ");
            	tweet = tweet.replaceAll("\r"," ");
            	tweet = tweet.replaceAll("\n"," ");
            	//ツイートからタブ記号を除去（半角スペースに変換）
            	tweet = tweet.replaceAll("\t"," ");
            	
            	//Visualization
            	System.out.println(name+username);
            	System.out.println(userId);
            	System.out.println(tweet);
            	System.out.println(createddate);
            	System.out.println(timezone);
            	System.out.println(userlang);
            	System.out.println(app);
            	System.out.println(profilegeo);
            	System.out.println(geo);            	
            }
        }

        public void onDeletionNotice(StatusDeletionNotice sdn) {
            System.out.println("onDeletionNotice.");
        }

        public void onTrackLimitationNotice(int i) {
            System.out.println("onTrackLimitationNotice.(" + i + ")");
        }

        public void onScrubGeo(long lat, long lng) {
            System.out.println("onScrubGeo.(" + lat + ", " + lng + ")");
        }

        public void onException(Exception excptn) {
            System.out.println("onException.");
        }

		@Override
		public void onStallWarning(StallWarning arg0) {
		}
		//文字列の中に日本語が含まれているかどうかのフィルター
		public static boolean containsNihongo(String str) {
	        for(int i = 0 ; i < str.length() ; i++) {
	            char ch = str.charAt(i);
	            Character.UnicodeBlock unicodeBlock = Character.UnicodeBlock.of(ch);
	            if (Character.UnicodeBlock.HIRAGANA.equals(unicodeBlock))
	                return true;
	            if (Character.UnicodeBlock.KATAKANA.equals(unicodeBlock))
	                return true;
	            if (Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS.equals(unicodeBlock))
	                return true;
	            if (Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS.equals(unicodeBlock))
	                return true;
	            if (Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION.equals(unicodeBlock))
	                return true;
	        }
	        return false;
	    }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
                .build();
        
        TwitterStream twStream = new TwitterStreamFactory(configuration).getInstance();
        twStream.addListener(new MyStatusListener());
        //String[] track = {"#daihyo"};
        //#JPNvs#COL
        //String[] track = {"#tvasahi,#daihyo,#jfa,#tvasahi_soccer,#JPN,#COL"};//soccer
        //String[] track = {"#nhk,#tvtokyo,#etv,#ntv,#tvasahi,#tbs,#fujitv,#tokyomx"};//TV
        String[] track = {"#nhk"};
        FilterQuery query = new FilterQuery();
		query.track(track);
        twStream.filter(query);
    }
}