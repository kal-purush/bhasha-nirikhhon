package architecture.community.board;

import java.util.Date;

import architecture.community.model.ModelObject;

public interface Board extends ModelObject {

	public abstract long getBoardId();

	public abstract long getObjectId();	
	
	public abstract String getName();
	
	public abstract String getDisplayName();
	
	public abstract String getDescription();
	
	public abstract Date getCreationDate();

	public abstract Date getModifiedDate();
	
}