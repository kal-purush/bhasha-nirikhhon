package seu.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import seu.domain.Class;
import seu.socket.Client;
import seu.socket.ClientRequest;

import java.util.List;

@Service
public class ClassService {

    private Client client;

    @Autowired
    public void setClient(Client client) {
        this.client = client;
    }

    private String serviceName;

    public ClassService() {
        serviceName = "classService";
    }

    public int insertClass(Class clss) {
        return (int) client.send(new ClientRequest(serviceName, "insertClass", new java.lang.Class[]{Class.class}, new Object[]{clss})).getData();
    }

    public int deleteClass(int classID) {
        return (int) client.send(new ClientRequest(serviceName, "deleteClass", new java.lang.Class[]{int.class}, new Object[]{classID})).getData();
    }

    public int updateClass(Class clss) {
        return (int) client.send(new ClientRequest(serviceName, "updateClass", new java.lang.Class[]{Class.class}, new Object[]{clss})).getData();
    }

    public Class queryClassByClassId(int classID) {
        return (Class) client.send(new ClientRequest(serviceName, "queryClassByClassId", new java.lang.Class[]{int.class}, new Object[]{classID})).getData();
    }

    public Class queryClassByStudentId(int studentID) {
        return (Class) client.send(new ClientRequest(serviceName, "queryClassByStudentId", new java.lang.Class[]{int.class}, new Object[]{studentID})).getData();
    }

    public List<Class> queryClassByTeacherId(int teacherID) {
        return (List<Class>) client.send(new ClientRequest(serviceName, "queryClassByTeacherId", new java.lang.Class[]{int.class}, new Object[]{teacherID})).getData();
    }

    public List<Class> queryAll() {
        return (List<Class>) client.send(new ClientRequest(serviceName, "queryAll")).getData();
    }
}