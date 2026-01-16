package rw.review.model.vo;

import java.sql.Date;

public class ReviewCard {
	private String reviewId;	// 리뷰 고유 ID
	private Date reviewDate;	// 리뷰 작성일
	private int reviewCount;	// 리뷰 조회수
	private int reviewRate;		// 리뷰 별점
	private String reviewCont;	// 리뷰 내용
	private String bookId;		// 책 고유 ID
	private String memberNo;	// 회원 고유 번호
	private char delYN;			// 리뷰 삭제 여부
	// 8개
	
	// private String memberNo; // 회원 고유 번호
	private String memberId;	// 회원 ID
	private String nickname;	// 회원 닉네임
	private String memberPwd;	// 회원 비번
	private String email;		// 회원 이메일
	private char emailYN;		// 회원 이메일 인증 YN
	private int birthYear;		// 회원 출생년도
	private char gender;		// 회원 성별
	private Date enrollDate;	// 가입일
	private char endYN;			// 탈퇴 YN
	private Date endDate;		// 탈퇴일
	private String profileImg;	// 프로필 이미지
	// 12개
	
	// private String bookId;		// 도서 고유 ID
	private String bookTitle;	// 도서 제목
	private String bookAuthor;	// 도서 저자
	private String bookImage;	// 도서 표지
	// 4개
	
	private String likeId;		// 좋아요 ID
	private char likeYN;		// 좋아요 YN
	// private String reviewId;	// 리뷰고유ID
	// private String memberNo;	// 내 멤버고유번호
	// 4개
	// 총 28개
	
	public ReviewCard() {
		super();
		// TODO Auto-generated constructor stub
	}
	public ReviewCard(String reviewId, Date reviewDate, int reviewCount, int reviewRate, String reviewCont,
			String bookId, String memberNo, char delYN, String memberId, String nickname, String memberPwd,
			String email, char emailYN, int birthYear, char gender, Date enrollDate, char endYN, Date endDate,
			String profileImg, String bookTitle, String bookAuthor, String bookImage, String likeId, char likeYN) {
		super();
		this.reviewId = reviewId;
		this.reviewDate = reviewDate;
		this.reviewCount = reviewCount;
		this.reviewRate = reviewRate;
		this.reviewCont = reviewCont;
		this.bookId = bookId;
		this.memberNo = memberNo;
		this.delYN = delYN;
		this.memberId = memberId;
		this.nickname = nickname;
		this.memberPwd = memberPwd;
		this.email = email;
		this.emailYN = emailYN;
		this.birthYear = birthYear;
		this.gender = gender;
		this.enrollDate = enrollDate;
		this.endYN = endYN;
		this.endDate = endDate;
		this.profileImg = profileImg;
		this.bookTitle = bookTitle;
		this.bookAuthor = bookAuthor;
		this.bookImage = bookImage;
		this.likeId = likeId;
		this.likeYN = likeYN;
	}
	public String getReviewId() {
		return reviewId;
	}
	public void setReviewId(String reviewId) {
		this.reviewId = reviewId;
	}
	public Date getReviewDate() {
		return reviewDate;
	}
	public void setReviewDate(Date reviewDate) {
		this.reviewDate = reviewDate;
	}
	public int getReviewCount() {
		return reviewCount;
	}
	public void setReviewCount(int reviewCount) {
		this.reviewCount = reviewCount;
	}
	public int getReviewRate() {
		return reviewRate;
	}
	public void setReviewRate(int reviewRate) {
		this.reviewRate = reviewRate;
	}
	public String getReviewCont() {
		return reviewCont;
	}
	public void setReviewCont(String reviewCont) {
		this.reviewCont = reviewCont;
	}
	public String getBookId() {
		return bookId;
	}
	public void setBookId(String bookId) {
		this.bookId = bookId;
	}
	public String getMemberNo() {
		return memberNo;
	}
	public void setMemberNo(String memberNo) {
		this.memberNo = memberNo;
	}
	public char getDelYN() {
		return delYN;
	}
	public void setDelYN(char delYN) {
		this.delYN = delYN;
	}
	public String getMemberId() {
		return memberId;
	}
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	public String getNickname() {
		return nickname;
	}
	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	public String getMemberPwd() {
		return memberPwd;
	}
	public void setMemberPwd(String memberPwd) {
		this.memberPwd = memberPwd;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public char getEmailYN() {
		return emailYN;
	}
	public void setEmailYN(char emailYN) {
		this.emailYN = emailYN;
	}
	public int getBirthYear() {
		return birthYear;
	}
	public void setBirthYear(int birthYear) {
		this.birthYear = birthYear;
	}
	public char getGender() {
		return gender;
	}
	public void setGender(char gender) {
		this.gender = gender;
	}
	public Date getEnrollDate() {
		return enrollDate;
	}
	public void setEnrollDate(Date enrollDate) {
		this.enrollDate = enrollDate;
	}
	public char getEndYN() {
		return endYN;
	}
	public void setEndYN(char endYN) {
		this.endYN = endYN;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public String getProfileImg() {
		return profileImg;
	}
	public void setProfileImg(String profileImg) {
		this.profileImg = profileImg;
	}
	public String getBookTitle() {
		return bookTitle;
	}
	public void setBookTitle(String bookTitle) {
		this.bookTitle = bookTitle;
	}
	public String getBookAuthor() {
		return bookAuthor;
	}
	public void setBookAuthor(String bookAuthor) {
		this.bookAuthor = bookAuthor;
	}
	public String getBookImage() {
		return bookImage;
	}
	public void setBookImage(String bookImage) {
		this.bookImage = bookImage;
	}
	public String getLikeId() {
		return likeId;
	}
	public void setLikeId(String likeId) {
		this.likeId = likeId;
	}
	public char getLikeYN() {
		return likeYN;
	}
	public void setLikeYN(char likeYN) {
		this.likeYN = likeYN;
	}
	
	
	
	
}