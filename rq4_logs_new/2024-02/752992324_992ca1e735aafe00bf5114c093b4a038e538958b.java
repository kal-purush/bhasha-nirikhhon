
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class UrlShortener {
    private Map<String, String> shortToLongMap;
    private Map<String, String> longToShortMap;

    public UrlShortener() {
        this.shortToLongMap = new HashMap<>();
        this.longToShortMap = new HashMap<>();
    }

    // Shorten URL and return the short code
    public String shortenUrl(String longUrl) {
        if (longToShortMap.containsKey(longUrl)) {
            return longToShortMap.get(longUrl);
        }

        String shortCode = generateShortCode();
        shortToLongMap.put(shortCode, longUrl);
        longToShortMap.put(longUrl, shortCode);

        return shortCode;
    }

    // Redirect to the original URL using the short code
    public String redirectToOriginalUrl(String shortCode) {
        return shortToLongMap.getOrDefault(shortCode, "URL not found");
    }

    // Generate a random short code
    private String generateShortCode() {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder shortCode = new StringBuilder();

        for (int i = 0; i < 6; i++) {
            shortCode.append(characters.charAt(random.nextInt(characters.length())));
        }

        return shortCode.toString();
    }

    public static void main(String[] args) {
        UrlShortener urlShortener = new UrlShortener();

        // Example: Shortening a URL
        String originalUrl = "https://www.amazon.com/";
        String shortCode = ": http://tinyurl.com/awq56oc";
        System.out.println("Shortened URL: " + shortCode);

        // Example: Redirecting to the original URL
        // String originalUrl = urlShortener.redirectToOriginalUrl(shortCode);
        System.out.println("Original URL: " + originalUrl);
    }
}