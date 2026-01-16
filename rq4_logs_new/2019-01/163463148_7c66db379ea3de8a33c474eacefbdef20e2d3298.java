/**
 * 
 */
package cn.edu.tit.bean;

/**
 * @author LiMing
 * 资源类型实体类
 */
public class ResourceType {
	private String resourceTypeId; 
	private String resourceType;
	@Override
	public String toString() {
		return "ResourceType [resourceTypeId=" + resourceTypeId + ", resourceType=" + resourceType + "]";
	}
	public String getResourceTypeId() {
		return resourceTypeId;
	}
	public void setResourceTypeId(String resourceTypeId) {
		this.resourceTypeId = resourceTypeId;
	}
	public String getResourceType() {
		return resourceType;
	}
	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}
	public ResourceType() {
		super();
		// TODO Auto-generated constructor stub
	}
	public ResourceType(String resourceTypeId, String resourceType) {
		super();
		this.resourceTypeId = resourceTypeId;
		this.resourceType = resourceType;
	}
}