package com.mph.chatcontrol.network;
/* Created by macmini on 29/11/2017. */

public interface RoomRealtimeService extends RoomService {

    interface RoomObserverCallback {

        void onRoomChanged(RestRoom room);

        void onError();
    }

    void observeRooms(Iterable<String> roomIDs, RoomObserverCallback callback);
}