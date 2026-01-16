package com.kakaotalk.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

import com.google.gson.Gson;
import com.kakaotalk.clientDto.CreateRespDto;
import com.kakaotalk.clientDto.ResponseDto;

import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor

public class ClientRecive extends Thread {
		
	private final Socket socket;
	private InputStream inputStream;
	private Gson gson;
	
		 @Override
		public void run() {
			
			 try {
				inputStream = socket.getInputStream();
				BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
				gson = new Gson();
				
				while(true) {
					
					String request = in.readLine();
					ResponseDto responseDto = gson.fromJson(request, ResponseDto.class);
					
					switch (responseDto.getResource()) {
					
						case "create" :
								CreateRespDto createRespDto = gson.fromJson(responseDto.getBody(), CreateRespDto.class);
								KakaoClient.getInstance().getChattingListModel().clear();
								KakaoClient.getInstance().getChattingListModel().addAll(createRespDto.getConnectedRooms());
								KakaoClient.getInstance().getChattingList().setSelectedIndex(0);
								break;
						//case "sendMessage" :
								//MessageRespDto messageRespDto = gson.fromJson(responseDto.getBody(),MessageRespDto.class);
								//KakaoClient.getInstance().getContentView().append(messageRespDto.getMessageValue()+ "\n");
					
					}
					
					
				}
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			 
			 
		}
}