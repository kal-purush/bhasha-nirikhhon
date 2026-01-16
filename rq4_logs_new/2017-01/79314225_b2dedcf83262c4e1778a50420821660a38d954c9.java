package cz.xelfi.demo.investicnihra.js;

import net.java.html.js.JavaScriptBody;

/** Use {@link JavaScriptBody} annotation on methods to
 * directly interact with JavaScript. See
 * http://bits.netbeans.org/html+java/1.2/net/java/html/js/package-summary.html
 * to understand how.
 */
public final class Utilities {
    private Utilities() {
    }
    
    @JavaScriptBody(args = { "toExecute" }, javacall = true, body = 
        "setTimeout(function() {" +
        "  toExecute.@java.lang.Runnable::run()();" +
        "}, 1000);"
    )
    public static native void afterASecond(Runnable toExecute);
}