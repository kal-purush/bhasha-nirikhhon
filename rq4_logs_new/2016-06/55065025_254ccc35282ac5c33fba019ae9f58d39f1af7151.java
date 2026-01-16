package es.unizar.tmdad.dbmodel;

import java.util.List;

public class BookAnalysis {
	private long id;
	private String title;
	private List<ChapterAnalysis> chapters;
	
	public BookAnalysis(long id, String title, List<ChapterAnalysis> chapters) {
		this.id = id;
		this.title = title;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<ChapterAnalysis> getChapters() {
		return chapters;
	}

	public void setChapters(List<ChapterAnalysis> chapters) {
		this.chapters = chapters;
	}
	
}