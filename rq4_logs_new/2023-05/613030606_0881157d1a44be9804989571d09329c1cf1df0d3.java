package ch.uzh.ifi.hase.soprafs23.service;

import ch.uzh.ifi.hase.soprafs23.entity.Message;
import ch.uzh.ifi.hase.soprafs23.entity.VoteMessage;
import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.protocol.Packet;
import javassist.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;

@SpringBootTest
public class SocketControllerHelperUnitTests {

    @Autowired
    private SocketControllerHelper socketControllerHelper;

    @BeforeEach
    public void Setup(){

    }

    @Test
    public void testSendRightAnswerMethod() throws NotFoundException {
        socketControllerHelper.sendRightAnswerMethod("123456");
    }

    @Test
    public void testUpdateVoteMethod() throws NotFoundException {
        socketControllerHelper.updateVoteMethod(new VoteMessage());
    }

    @Test
    public void testRequestRankingMethod() throws NotFoundException {
        socketControllerHelper.requestRankingMethod(new Message());
    }

    @Test
    public void testJoinRoomMethod(){
        SocketIOClient socketIOClient = new SocketIOClient() {
            @Override
            public HandshakeData getHandshakeData() {
                return null;
            }

            @Override
            public Transport getTransport() {
                return null;
            }

            @Override
            public void sendEvent(String name, AckCallback<?> ackCallback, Object... data) {

            }

            @Override
            public void send(Packet packet, AckCallback<?> ackCallback) {

            }

            @Override
            public SocketIONamespace getNamespace() {
                return null;
            }

            @Override
            public UUID getSessionId() {
                return null;
            }

            @Override
            public SocketAddress getRemoteAddress() {
                return null;
            }

            @Override
            public boolean isChannelOpen() {
                return false;
            }

            @Override
            public void joinRoom(String room) {

            }

            @Override
            public void joinRooms(Set<String> rooms) {

            }

            @Override
            public void leaveRoom(String room) {

            }

            @Override
            public void leaveRooms(Set<String> rooms) {

            }

            @Override
            public Set<String> getAllRooms() {
                return null;
            }

            @Override
            public int getCurrentRoomSize(String room) {
                return 0;
            }

            @Override
            public void send(Packet packet) {

            }

            @Override
            public void disconnect() {

            }

            @Override
            public void sendEvent(String name, Object... data) {

            }

            @Override
            public void set(String key, Object val) {

            }

            @Override
            public <T> T get(String key) {
                return null;
            }

            @Override
            public boolean has(String key) {
                return false;
            }

            @Override
            public void del(String key) {

            }
        };
        socketControllerHelper.joinRoomMethod(socketIOClient, new Message());
    }

}