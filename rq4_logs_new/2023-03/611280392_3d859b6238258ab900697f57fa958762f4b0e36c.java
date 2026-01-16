package LeetCode;

import java.util.LinkedList;
import java.util.List;

/**
 * https://leetcode.com/problems/design-browser-history/description/
 *
 * 1472. Design Browser History
 * Medium
 *
 * You have a browser of one tab where you start on the homepage and you can visit another url, get back in the history number of steps or move forward in the history number of steps.
 *
 * Implement the BrowserHistory class:
 *
 *     BrowserHistory(string homepage) Initializes the object with the homepage of the browser.
 *     void visit(string url) Visits url from the current page. It clears up all the forward history.
 *     string back(int steps) Move steps back in history. If you can only return x steps in the history and steps > x, you will return only x steps. Return the current url after moving back in history at most steps.
 *     string forward(int steps) Move steps forward in history. If you can only forward x steps in the history and steps > x, you will forward only x steps. Return the current url after forwarding in history at most steps.
 *
 *
 *
 * Example:
 *
 * Input:
 * ["BrowserHistory","visit","visit","visit","back","back","forward","visit","forward","back","back"]
 * [["leetcode.com"],["google.com"],["facebook.com"],["youtube.com"],[1],[1],[1],["linkedin.com"],[2],[2],[7]]
 * Output:
 * [null,null,null,null,"facebook.com","google.com","facebook.com",null,"linkedin.com","google.com","leetcode.com"]
 *
 * Explanation:
 * BrowserHistory browserHistory = new BrowserHistory("leetcode.com");
 * browserHistory.visit("google.com");       // You are in "leetcode.com". Visit "google.com"
 * browserHistory.visit("facebook.com");     // You are in "google.com". Visit "facebook.com"
 * browserHistory.visit("youtube.com");      // You are in "facebook.com". Visit "youtube.com"
 * browserHistory.back(1);                   // You are in "youtube.com", move back to "facebook.com" return "facebook.com"
 * browserHistory.back(1);                   // You are in "facebook.com", move back to "google.com" return "google.com"
 * browserHistory.forward(1);                // You are in "google.com", move forward to "facebook.com" return "facebook.com"
 * browserHistory.visit("linkedin.com");     // You are in "facebook.com". Visit "linkedin.com"
 * browserHistory.forward(2);                // You are in "linkedin.com", you cannot move forward any steps.
 * browserHistory.back(2);                   // You are in "linkedin.com", move back two steps to "facebook.com" then to "google.com". return "google.com"
 * browserHistory.back(7);                   // You are in "google.com", you can move back only one step to "leetcode.com". return "leetcode.com"
 *
 *
 *
 * Constraints:
 *
 *     1 <= homepage.length <= 20
 *     1 <= url.length <= 20
 *     1 <= steps <= 100
 *     homepage and url consist of  '.' or lower case English letters.
 *     At most 5000 calls will be made to visit, back, and forward.
 */
public class _1472_Medium_Design_Browser_History
{
	public static void main(String[] args) throws Exception
	{
		BrowserHistory obj = new BrowserHistory("leetcode.com");
		System.out.println(obj.visit("google.com"));
		System.out.println(obj.visit("facebook.com"));
		System.out.println(obj.visit("youtube.com"));
		System.out.println(obj.back(1));
		System.out.println(obj.back(1));
		System.out.println(obj.forward(1));
		System.out.println(obj.visit("linkedin.com"));
		System.out.println(obj.forward(2));
		System.out.println(obj.back(2));
		System.out.println(obj.back(7));

		obj = new BrowserHistory("momn.com");
		System.out.println(obj.visit("bx.com"));
		System.out.println(obj.visit("bjyfmln.com"));
		System.out.println(obj.back(3));
		System.out.println(obj.visit("ijtrqk.com"));
		System.out.println(obj.visit("dft.com"));
		System.out.println(obj.back(10));
		System.out.println(obj.forward(10));
		System.out.println(obj.visit("yc.com"));
		System.out.println(obj.visit("yhl.com"));
		System.out.println(obj.visit("xynxvix.com"));
		System.out.println(obj.visit("izfscdv.com"));
		System.out.println(obj.visit("cdenhm.com"));
		System.out.println(obj.visit("ocgcjz.com"));
		System.out.println(obj.forward(5));
		System.out.println(obj.forward(5));
		System.out.println(obj.visit("gtd.com"));
		System.out.println(obj.back(9));
		System.out.println(obj.visit("hfeour.com"));
		System.out.println(obj.visit("ghmh.com"));
		System.out.println(obj.visit("nnm.com"));
		System.out.println(obj.visit("knm.com"));
		System.out.println(obj.forward(4));
		System.out.println(obj.visit("cbtg.com"));
		System.out.println(obj.visit("acyvwod.com"));
		System.out.println(obj.visit("mydr.com"));
	}

	static class BrowserHistory
	{
		List<String> browserHistory = new LinkedList<>();
		int pos = -1;

		public BrowserHistory(String homepage)
		{
			browserHistory.add(homepage);
			pos++;
		}

		public int visit(String url)
		{
			if(browserHistory.size() > pos + 1)
			{
				browserHistory.subList(pos + 1, browserHistory.size()).clear();
			}
			browserHistory.add(url);
			pos++;
			return pos;
		}

		public String back(int steps)
		{
			if(((pos + 1) - steps) <= 0)
			{
				pos = 0;
				return browserHistory.get(pos);
			}
			pos = pos - steps;
			return browserHistory.get(pos);
		}

		public String forward(int steps)
		{
			if(((pos + 1) + steps) >= browserHistory.size())
			{
				pos = browserHistory.size() - 1;
				return browserHistory.get(pos);
			}
			pos = pos + steps;
			return browserHistory.get(pos);
		}
	}
}