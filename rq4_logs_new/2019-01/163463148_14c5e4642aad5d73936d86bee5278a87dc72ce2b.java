package cn.edu.tit.bean;

/**
 * @author LiMing
 *
 */
public class Term {
	private String termId;
	private String startYear;
	private String endYear;
	private String termOne;
	private String termTwo;
	public String getTermId() {
		return termId;
	}
	public void setTermId(String termId) {
		this.termId = termId;
	}
	public String getStartYear() {
		return startYear;
	}
	public void setStartYear(String startYear) {
		this.startYear = startYear;
	}
	public String getEndYear() {
		return endYear;
	}
	public void setEndYear(String endYear) {
		this.endYear = endYear;
	}
	public String getTermOne() {
		return termOne;
	}
	public void setTermOne(String termOne) {
		this.termOne = termOne;
	}
	public String getTermTwo() {
		return termTwo;
	}
	public void setTermTwo(String termTwo) {
		this.termTwo = termTwo;
	}
	public Term() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Term(String termId, String startYear, String endYear, String termOne, String termTwo) {
		super();
		this.termId = termId;
		this.startYear = startYear;
		this.endYear = endYear;
		this.termOne = termOne;
		this.termTwo = termTwo;
	}
	@Override
	public String toString() {
		return "Term [termId=" + termId + ", startYear=" + startYear + ", endYear=" + endYear + ", termOne=" + termOne
				+ ", termTwo=" + termTwo + "]";
	}
	
}