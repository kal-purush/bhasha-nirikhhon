package com.example.tagletagle.src.notification.dto;

import com.example.tagletagle.config.NotificationType;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Object Mapper을 통해 json 받아올 DTO")
@AllArgsConstructor
@NoArgsConstructor
public class NotificationInfoDTO {

	@Schema(description = "알람 종류", nullable = false, allowableValues = {"FOLLOW", "LIKE", "SAVE"},example = "SAVE")
	private NotificationType notificationType;

	@Schema(description = "상대방 user_id", nullable = false)
	private Long relatedUserId;

	@Schema(description = "관련 post_id(팔로우 알림 시에는 null 값)", nullable = true)
	private Long relatedPostId;

	@Schema(description = "읽음 표시", nullable = false)
	private Boolean isRead;






}