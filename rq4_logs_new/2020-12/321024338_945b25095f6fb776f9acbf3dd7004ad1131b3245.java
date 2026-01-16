package rw.library.model.vo;

public class Library {
	private String bookShelfId;
	private String bookShelfName;
	private String memberNo;
	private char private_YN;
	private char del_YN;
	
	public Library() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Library(String bookShelfId, String bookShelfName, String memberNo, char private_YN, char del_YN) {
		super();
		this.bookShelfId = bookShelfId;
		this.bookShelfName = bookShelfName;
		this.memberNo = memberNo;
		this.private_YN = private_YN;
		this.del_YN = del_YN;
	}
	public String getBookShelfId() {
		return bookShelfId;
	}
	public void setBookShelfId(String bookShelfId) {
		this.bookShelfId = bookShelfId;
	}
	public String getBookShelfName() {
		return bookShelfName;
	}
	public void setBookShelfName(String bookShelfName) {
		this.bookShelfName = bookShelfName;
	}
	public String getMemberNo() {
		return memberNo;
	}
	public void setMemberNo(String memberNo) {
		this.memberNo = memberNo;
	}
	public char getPrivate_YN() {
		return private_YN;
	}
	public void setPrivate_YN(char private_YN) {
		this.private_YN = private_YN;
	}
	public char getDel_YN() {
		return del_YN;
	}
	public void setDel_YN(char del_YN) {
		this.del_YN = del_YN;
	}
	
}

