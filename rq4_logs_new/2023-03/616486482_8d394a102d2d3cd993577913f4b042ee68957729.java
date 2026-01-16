package blog.model;

import java.util.Date;

public class PlayList{
	protected long playListId;
	protected User user;
	protected Date created;
	
	public PlayList(long playListId,User user,Date created) {
		this.playListId = playListId;
		this.user = user;
		this.created = created;
	}
	
	public PlayList(long playListId) {
		this.playListId = playListId;
	}
	
	public PlayList(User user,Date created) {
		this.user = user;
		this.created = created;
	}
	
	public long getPlayListId() {
		return playListId;
	}
	
	public void setPlayListId(long playListId) {
		this.playListId = playListId;
	}
	
	public User getUser() {
		return user;
	}
	
	public void setUser(User user) {
		this.user = user;
	}
	
	public Date getCreated() {
		return created;
	}
	
	public void setCreated(Date created) {
		this.created = created;
	}
}