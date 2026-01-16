package pl.put.poznan.networkanalyzer.service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import pl.put.poznan.networkanalyzer.model.Connection;
import pl.put.poznan.networkanalyzer.model.Node;
import pl.put.poznan.networkanalyzer.model.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class NetAnalServiceTest {

    private static final String TYP = "EXAMPLE_TYPE";
    private static final String NAME = "EXAMPLE_NAME";

    @Mock
    public NodeService nodeService;

    @Mock
    public ConnectionService connectionService;

    private NetAnalService netAnalService;

    private ArrayList<Node> nodeList = new ArrayList<>();
    private ArrayList<Connection> connectionList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        connectionList.add(new Connection(1,2,10));
        connectionList.add(new Connection(1,3,10));
        connectionList.add(new Connection(1,4,10));
        connectionList.add(new Connection(3,5,10));
        connectionList.add(new Connection(4,5,10));

        Mockito.when(connectionService.getAllConnections())
                .thenReturn(connectionList);

        for (int i = 0; i < 5; i++) {
            nodeList.add(new Node(i+1,TYP,NAME,new ArrayList<>(),new ArrayList<>()));
        }

        for (int i = 0; i < 5; i++) {
            setIO(connectionList.get(i));
        }

        Mockito.when(nodeService.getAllNodes())
                .thenReturn(nodeList);

        netAnalService = new NetAnalService(connectionService,nodeService);
    }

    @Test
    public void bfsTest_bfsType() {
        Mockito.when(connectionService.getConnectionByNodes(1,2)).thenReturn(connectionList.get(0));
        Mockito.when(connectionService.getConnectionByNodes(1,3)).thenReturn(connectionList.get(1));
        Mockito.when(connectionService.getConnectionByNodes(1,4)).thenReturn(connectionList.get(2));
        Mockito.when(connectionService.getConnectionByNodes(3,5)).thenReturn(connectionList.get(3));
        Mockito.when(connectionService.getConnectionByNodes(4,5)).thenReturn(connectionList.get(4));

        for (int i = 0; i < 5; i++) {
            Mockito.when(nodeService.getOneNode(i)).thenReturn(nodeList.get(i));
        }

        ArrayList<Integer> givenPath = netAnalService.BFSPath(1,5,"BFS");
        ArrayList<Integer> expectedPath = new ArrayList<>();
        expectedPath.add(5);
        expectedPath.add(4);
        expectedPath.add(1);

        assertEquals(expectedPath,givenPath);
    }

    private void setIO(Connection connection) {
        Integer outNode = connection.getFrom();
        Integer inNode = connection.getTo();

        for (Node node: nodeList) {
            if(node.getId().equals(outNode)){
                node.addOutgoing(connection.getTo());
            }else if(node.getId().equals(inNode)){
                node.addIncoming(connection.getFrom());
            }
        }
    }
}
