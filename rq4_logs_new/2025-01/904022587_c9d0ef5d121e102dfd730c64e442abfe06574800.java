package com.green.manager.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ManagerCommunityImageVo {

	private int     community_image_idx;
	private int     community_idx;
	private String  community_image_name; 
	private String  community_image_ext;
	private String  community_sfile_name;

}