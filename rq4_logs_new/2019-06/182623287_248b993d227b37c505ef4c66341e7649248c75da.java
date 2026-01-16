package movie.beans;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

public class MovieListBeans implements Serializable {
	private String movieId;
	private int adminNum;
	private String movieName;
	private String cast;
	private String directed;
	private Date startDate;
	private Date finishDate;
	private String detail;
	private Timestamp termStart;
	private Timestamp termFinish;
	private int count;
	private int sheet;

	public String getMovieId() {
		return movieId;
	}
	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}
	public int getAdminNum() {
		return adminNum;
	}
	public void setAdminNum(int adminNum) {
		this.adminNum = adminNum;
	}
	public String getMovieName() {
		return movieName;
	}
	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}
	public String getCast() {
		return cast;
	}
	public void setCast(String cast) {
		this.cast = cast;
	}
	public String getDirected() {
		return directed;
	}
	public void setDirected(String directed) {
		this.directed = directed;
	}
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getFinishDate() {
		return finishDate;
	}
	public void setFinishDate(Date finishDate) {
		this.finishDate = finishDate;
	}
	public String getDetail() {
		return detail;
	}
	public void setDetail(String detail) {
		this.detail = detail;
	}
	public Timestamp getTermStart() {
		return termStart;
	}
	public void setTermStart(Timestamp termStart) {
		this.termStart = termStart;
	}
	public Timestamp getTermFinish() {
		return termFinish;
	}
	public void setTermFinish(Timestamp termFinish) {
		this.termFinish = termFinish;
	}

	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getSheet() {
		return sheet;
	}
	public void setSheet(int sheet) {
		this.sheet = sheet;
	}


}