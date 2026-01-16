package com.example.spoot_taxi_front.network.api;

import com.example.spoot_taxi_front.network.dto.responses.ChatRoomMessageResponse;
import com.example.spoot_taxi_front.network.dto.responses.UserJoinedChatRoomResponse;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface ChatApi {
    //유저가 참가한 ChatRoom의 이름과 Id 반환 API
    @GET("/api/chat/user/chatRooms")
    Call<UserJoinedChatRoomResponse> getUserChatRooms(@Query("email")String email);

    @GET("/api/chat/chatRoom/messages")
    Call<ChatRoomMessageResponse> getChatRoomMessages(@Query("userEmail")String userEmail,@Query("chatRoomId")Long chatRoomId);
}