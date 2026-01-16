package bean;

public class CourseBean {
	private String courseID;
	private String teacherID;
	private String launchDate;
	private String launchTime;
	private float length;
	private String type;
	private String instrument;
	private String roomID;
	private float expense;
	private int volume;
	private String other;

	public CourseBean(String courseID, String teacherID, String launchDate,
			String launchTime, float length, String type, String instrument,
			String roomID, float expense, int volume, String other) {
		super();
		this.courseID = courseID;
		this.teacherID = teacherID;
		this.launchDate = launchDate;
		this.launchTime = launchTime;
		this.length = length;
		this.type = type;
		this.instrument = instrument;
		this.roomID = roomID;
		this.expense = expense;
		this.volume = volume;
		this.other = other;
	}

	public String getCourseID() {
		return courseID;
	}

	public void setCourseID(String courseID) {
		this.courseID = courseID;
	}

	public String getTeacherID() {
		return teacherID;
	}

	public void setTeacherID(String teacherID) {
		this.teacherID = teacherID;
	}

	public String getLaunchDate() {
		return launchDate;
	}

	public void setLaunchDate(String launchDate) {
		this.launchDate = launchDate;
	}

	public String getLaunchTime() {
		return launchTime;
	}

	public void setLaunchTime(String launchTime) {
		this.launchTime = launchTime;
	}

	public float getLength() {
		return length;
	}

	public void setLength(float length) {
		this.length = length;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getInstrument() {
		return instrument;
	}

	public void setInstrument(String instrument) {
		this.instrument = instrument;
	}

	public String getRoomID() {
		return roomID;
	}

	public void setRoomID(String roomID) {
		this.roomID = roomID;
	}

	public float getExpense() {
		return expense;
	}

	public void setExpense(float expense) {
		this.expense = expense;
	}

	public int getVolume() {
		return volume;
	}

	public void setVolume(int volume) {
		this.volume = volume;
	}

	public String getOther() {
		return other;
	}

	public void setOther(String other) {
		this.other = other;
	}

}