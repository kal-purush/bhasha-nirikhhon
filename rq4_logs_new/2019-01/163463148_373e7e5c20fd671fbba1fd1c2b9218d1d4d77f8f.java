/**
 * 课程相关的签到类
 */
package cn.edu.tit.bean;

public class SignIn {

	private String signInId; // 签到id
	private String courceId; // 课程id
	private Integer signPoint; // 签到积分
	public String getSignInId() {
		return signInId;
	}
	public void setSignInId(String signInId) {
		this.signInId = signInId;
	}
	public String getCourceId() {
		return courceId;
	}
	public void setCourceId(String courceId) {
		this.courceId = courceId;
	}
	public Integer getSignPoint() {
		return signPoint;
	}
	public void setSignPoint(Integer signPoint) {
		this.signPoint = signPoint;
	}
	@Override
	public String toString() {
		return "SignIn [signInId=" + signInId + ", courceId=" + courceId + ", signPoint=" + signPoint + "]";
	}
	public SignIn(String signInId, String courceId, Integer signPoint) {
		super();
		this.signInId = signInId;
		this.courceId = courceId;
		this.signPoint = signPoint;
	}
	
	
	
}