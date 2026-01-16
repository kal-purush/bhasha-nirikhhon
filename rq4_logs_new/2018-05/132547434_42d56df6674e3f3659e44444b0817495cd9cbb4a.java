package id.co.javan.webscraper;

/**
 * This strategy assumes phantom executable are in PATH.
 */
public class PathResolutionStrategy implements PhantomJsResolutionStrategy {

    public String getPath() {
        return "phantomjs";
    }
}