package rw.faq.model.vo;

import java.sql.Date;

public class Faq {
	private int faqNo;
	private String faqTitle;
	private Date faqDate;
	private String faqCont;
	private char delYN;

	public int getFaqNo() {
		return faqNo;
	}

	public void setFaqNo(int faqNo) {
		this.faqNo = faqNo;
	}

	public String getFaqTitle() {
		return faqTitle;
	}

	public void setFaqTitle(String faqTitle) {
		this.faqTitle = faqTitle;
	}

	public Date getFaqDate() {
		return faqDate;
	}

	public void setFaqDate(Date faqDate) {
		this.faqDate = faqDate;
	}

	public String getFaqCont() {
		return faqCont;
	}

	public void setFaqCont(String faqCont) {
		this.faqCont = faqCont;
	}

	public char getDelYN() {
		return delYN;
	}

	public void setDelYN(char delYN) {
		this.delYN = delYN;
	}

	public Faq() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Faq(int faqNo, String faqTitle, Date faqDate, String faqCont, char delYN) {
		super();
		this.faqNo = faqNo;
		this.faqTitle = faqTitle;
		this.faqDate = faqDate;
		this.faqCont = faqCont;
		this.delYN = delYN;
	}

}