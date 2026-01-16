package aims.media;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Book extends Media{
	private List<String> authors = new ArrayList<String>();
	
	public Book(int id, String title, String category, float cost, List<String> authors) {
		super(id, title, category, cost);
		this.authors = authors;
	}
	
	public List<String> getAuthors() {
		return authors;
	}
	public void setAuthors(List<String> authors) {
		this.authors = authors;
	}
	
	public void addAuthor(String authorName)
	{
		for(String s : authors)
		{
			if(s.equals(authorName) == true)
			{
				System.out.println("This author already existed");
				return;
			}
		}
		authors.add(authorName);
		System.out.println("Author is added sucessfully");
	}
	
	public void removeAuthor(String authorName)
	{
		Iterator<String> s = this.authors.iterator();
		while(s.hasNext() == true)
		{
			if(authorName.equals(s.next()) == true)
			{
				s.remove();
				System.out.println("The author is removed");
				return;
			}
		}
		System.out.println("The author doesnot exist");
	}
	
	
}