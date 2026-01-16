package centauri.academy.proxima.cerepro.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name="candidates")
public class Candidates implements Serializable, EntityInterface{

	private static final long serialVersionUID = -8768834971213658552L;
	
	@Id
	@Column(name="id")
	@GeneratedValue(generator="increment")
	@GenericGenerator(name="increment", strategy = "increment")
	private Long id;
	
	@Column(name="user_id")
	private Long user_id;
	
	@Column(name="course_code")
	private String course_code;
	
	@Column(name="candidacy_date_time")
	private Timestamp candidacy_date_time;

	@Column(name="firstname")
	private String firstname;
	
	@Column(name="lastname")
	private String lastname;
	
	@Column(name="email")
	private String email;
	
	@Column(name="regdate")
	private Timestamp regdate;
	
	@Column(name="inserted_by")
	private Long inserted_by;
	
	@Column(name="candidate_state_code")
	private Long candidate_state_code;
	
	/**
	 * 
	 * */
	public Candidates() {
		super();
	}

	public Candidates(Long id, Long user_id, String course_code, Timestamp candidacy_date_time, String firstname,
			String lastname, String email, Timestamp regdate, Long inserted_by, Long candidate_state_code) {
		super();
		this.id = id;
		this.user_id = user_id;
		this.course_code = course_code;
		this.candidacy_date_time = candidacy_date_time;
		this.firstname = firstname;
		this.lastname = lastname;
		this.email = email;
		this.regdate = regdate;
		this.inserted_by = inserted_by;
		this.candidate_state_code = candidate_state_code;
	}

	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * @return the user_id
	 */
	public Long getUser_id() {
		return user_id;
	}

	/**
	 * @param user_id the user_id to set
	 */
	public void setUser_id(Long user_id) {
		this.user_id = user_id;
	}

	/**
	 * @return the course_code
	 */
	public String getCourse_code() {
		return course_code;
	}

	/**
	 * @param course_code the course_code to set
	 */
	public void setCourse_code(String course_code) {
		this.course_code = course_code;
	}

	/**
	 * @return the candidacy_date_time
	 */
	public Timestamp getCandidacy_date_time() {
		return candidacy_date_time;
	}

	/**
	 * @param candidacy_date_time the candidacy_date_time to set
	 */
	public void setCandidacy_date_time(Timestamp candidacy_date_time) {
		this.candidacy_date_time = candidacy_date_time;
	}

	/**
	 * @return the firstname
	 */
	public String getFirstname() {
		return firstname;
	}

	/**
	 * @param firstname the firstname to set
	 */
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	/**
	 * @return the lastname
	 */
	public String getLastname() {
		return lastname;
	}

	/**
	 * @param lastname the lastname to set
	 */
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}
	
	/**
	 * @return the regdate
	 */
	public Timestamp getRegdate() {
		return regdate;
	}

	/**
	 * @param email the email to set
	 */
	public void setRegdate(Timestamp regdate) {
		this.regdate = regdate;
	}

	/**
	 * @return the inserted_by
	 */
	public Long getInserted_by() {
		return inserted_by;
	}

	/**
	 * @param inserted_by the inserted_by to set
	 */
	public void setInserted_by(Long inserted_by) {
		this.inserted_by = inserted_by;
	}

	/**
	 * @return the candidate_state_code
	 */
	public Long getCandidate_state_code() {
		return candidate_state_code;
	}

	/**
	 * @param candidate_state_code the candidate_state_code to set
	 */
	public void setCandidate_state_code(Long candidate_state_code) {
		this.candidate_state_code = candidate_state_code;
	}

	@Override
	public String toString() {
		return "Candidates [id=" + id + ", user_id=" + user_id + ", course_code=" + course_code
				+ ", candidacy_date_time=" + candidacy_date_time + ", firstname=" + firstname + ", lastname=" + lastname
				+ ", email=" + email + ", regdate=" + regdate + ", inserted_by=" + inserted_by + ", candidate_state_code=" + candidate_state_code
				+ "]";
	}
}