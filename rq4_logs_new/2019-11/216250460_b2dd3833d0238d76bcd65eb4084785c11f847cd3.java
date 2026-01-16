package System;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class findPathTest {

    @InjectMocks
    private findPath findPath;

    @Test
    public void getNode(){
        findPath.grid = new findPath.Cell[2][2];
        findPath.getNode(10,10);
    }
}