package firework.hyl.running.web.action.member;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;

import com.opensymphony.xwork2.ActionSupport;

import firework.hyl.running.common.bean.Memberinfo;
import firework.hyl.running.common.bean.Memberspace;
import firework.hyl.running.common.exception.MemberServiceException;
import firework.hyl.running.service.IMemberService;

@Controller("showSpaceAction")
@Scope("prototype")
public class ShowSpaceAction extends ActionSupport {

	private static final long serialVersionUID = 1L;

	private String nickName;
	private Memberspace space;
	private String spaceMsg;
	private Long memberId;
	private Memberinfo friend;
	@Autowired
	private IMemberService memberService;

	@Override
	public String execute() throws Exception {
		System.out.println("name:"+nickName);
		try {
			if (!this.memberService.haveMemberSpace(nickName))
				throw new MemberServiceException("【" + nickName + "】尚未开通个人空间");
		} catch (Exception e) {
			this.spaceMsg = e.getMessage();
			this.spaceMsg="对方没开通空间";
			return "no_space";
		}

		this.space = this.memberService.findMemberSapceByUserId(memberId);

		this.friend = this.memberService.findMemberinfoByName(nickName);
		System.out.println("FFFFFFFF:"+friend);
		return SUCCESS;
	}

	public String getNickName() {
		return nickName;
	}

	public void setNickName(String nickName) {
		this.nickName = nickName;
	}

	public Memberspace getSpace() {
		return space;
	}

	public void setSpace(Memberspace space) {
		this.space = space;
	}

	public Long getMemberId() {
		return memberId;
	}

	public void setMemberId(Long memberId) {
		this.memberId = memberId;
	}

	public String getSpaceMsg() {
		return spaceMsg;
	}

	public void setSpaceMsg(String spaceMsg) {
		this.spaceMsg = spaceMsg;
	}

	public Memberinfo getFriend() {
		return friend;
	}

	public void setFriend(Memberinfo friend) {
		this.friend = friend;
	}

}