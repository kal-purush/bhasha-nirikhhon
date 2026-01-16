/**
 * 章节目录类
 */
package cn.edu.tit.bean;

public class Chapter {
	
	private Integer chapterId; // 目录id
	private String chapterName; // 目录名
	private Integer superiorChapter; // 所属上级目录
	public Integer getChapterId() {
		return chapterId;
	}
	public void setChapterId(Integer chapterId) {
		this.chapterId = chapterId;
	}
	public String getChapterName() {
		return chapterName;
	}
	public void setChapterName(String chapterName) {
		this.chapterName = chapterName;
	}
	public Integer getSuperiorChapter() {
		return superiorChapter;
	}
	public void setSuperiorChapter(Integer superiorChapter) {
		this.superiorChapter = superiorChapter;
	}
	@Override
	public String toString() {
		return "Chapter [chapterId=" + chapterId + ", chapterName=" + chapterName + ", superiorChapter="
				+ superiorChapter + "]";
	}
	public Chapter(Integer chapterId, String chapterName, Integer superiorChapter) {
		super();
		this.chapterId = chapterId;
		this.chapterName = chapterName;
		this.superiorChapter = superiorChapter;
	}
	
	
}