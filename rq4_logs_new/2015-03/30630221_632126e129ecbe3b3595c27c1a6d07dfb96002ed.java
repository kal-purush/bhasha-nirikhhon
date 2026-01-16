package test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;

import base.Post;
import base.PostSortByContentLength;
import base.User;
import blog.Blog;

public class TestIO {

	public static void main(String[] args) {

          System.out.println("****Creating a blog...******");
		User user = new User(1, "A", "a@cse.ust.hk");
		
		ArrayList<Post> allposts = new ArrayList<Post>();
		// Month starts with 0, not 1
		Calendar cal1 = new GregorianCalendar(2015, 02, 1); // March 1, 2015
		allposts.add(new Post(cal1.getTime(), "No sunshine these days"));

		Calendar cal4 = new GregorianCalendar(2014, 11, 25); // Dec. 25, 2014
		allposts.add(new Post(cal4.getTime(), "Merry Xmas @Amy!"));

		Calendar cal3 = new GregorianCalendar(2015, 01, 2); // Feb 2, 2015
		allposts.add(new Post(cal3.getTime(),
				"New semester starts. Fighting!!!"));

		Calendar cal2 = new GregorianCalendar(2015, 01, 18); // Feb 18, 2015
		allposts.add(new Post(cal2.getTime(),
				"Thanks @Amy for your beautiful gift"));
		
		Blog blog=new Blog(user);
		blog.setPosts(allposts);
          blog.list();
		
		
		// *****************Task 1: save a blog to a file*************
		System.out.println("\n***Saving the blog to an external file***");
		String savefilepath="C:/Users/bericafort.CSD/"+user.getUserName()+".blog";
		//if the path is not accessible, change it to any path you can access
		System.out.println("File path: "+savefilepath);
		blog.save(savefilepath);
		
		
		
		
		/** 
		 * Now please move the file you saved to another place, say D:/
		 */
		
		

		// ******************Task 2: load a blog from a file***********
		// Comment the code below if you can't finish this task
		System.out.println("\n***Loading a blog from an external file...***");
		String loadfilepath="C:/Users/bericafort.CSD/"+user.getUserName()+".blog";
		    //if the path is not accessible, change it to any path you can access
		System.out.println("File path: "+loadfilepath);
		Blog newblog = new Blog(user);
		newblog.load(loadfilepath);
		newblog.list();
		
		
		// ******************Task 3: Error handling *************
		// Comment the code below if you can't finish this task
		System.out.println("\n***Error handling..***");
		Blog anyblog = new Blog(user);
		anyblog.load("D:/non-exist.blog");

	}

}