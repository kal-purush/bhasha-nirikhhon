package com.zhaoxuan.wehome.framework.model.impl;

import com.zhaoxuan.cakedao.AbstractCakeDao;
import com.zhaoxuan.cakedao.CakeDao;
import com.zhaoxuan.wehome.module.event.ChatEvent;
import com.zhaoxuan.wehome.module.event.FamilyPhotoEvent;
import com.zhaoxuan.wehome.module.manager.SharedManager;
import com.zhaoxuan.wehome.support.constants.Ints;
import com.zhaoxuan.wehome.support.dispensebus.DispenseBus;
import com.zhaoxuan.wehome.support.dto.ChatDto;
import com.zhaoxuan.wehome.support.utils.NetUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by lizhaoxuan on 16/6/18.
 */
public class ChatModel {

    private DispenseBus dispenseBus = DispenseBus.getInstance();
    private AbstractCakeDao<ChatDto> chatDao = CakeDao.getCakeDao(ChatDto.class);

    public void getData() {
        String photoPath = SharedManager.getFamilyPhoto();
        dispenseBus.post(new FamilyPhotoEvent(true, photoPath, ""));
        if (NetUtil.isConnectingToInternet()) {
            dispenseBus.post(new ChatEvent(true, Ints.DATA_LOAD, getChatList(), ""));
        } else {
            dispenseBus.post(new ChatEvent(false, Ints.DATA_LOAD, null, "网络请求失败，请稍后重试"));
        }
    }

    public void setFamilyPhoto(String path) {
        if (NetUtil.isConnectingToInternet()) {
            SharedManager.setFamilyPhoto(path);
            dispenseBus.post(new FamilyPhotoEvent(true, path, "修改全家福照片成功"));
        } else {
            dispenseBus.post(new FamilyPhotoEvent(false, "", "网络请求失败，请稍后重试"));
        }
    }

    public void addChatData(ChatDto chatDto) {
        if (NetUtil.isConnectingToInternet()) {
            long result = chatDao.insert(chatDto);
            if (result >= 0) {
                dispenseBus.post(new ChatEvent(true, Ints.DATA_ADD, getChatList(), "新建成功"));
            } else {
                dispenseBus.post(new ChatEvent(false, Ints.DATA_ADD, null, "新建失败"));
            }
        } else {
            dispenseBus.post(new ChatEvent(false, Ints.DATA_ADD, null, "网络请求失败，请稍后重试"));
        }


    }

    private List<ChatDto> getChatList() {
        List<ChatDto> list = new ArrayList<>();
        ChatDto[] chatDtos = chatDao.loadAllData();
        if (chatDtos != null && chatDtos.length != 0) {
            Collections.addAll(list, chatDtos);
        }
        return list;
    }
}