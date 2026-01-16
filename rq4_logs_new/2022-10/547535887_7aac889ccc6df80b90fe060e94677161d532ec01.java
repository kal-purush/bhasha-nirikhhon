import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import roo2.BouncingIndex;
import roo2.RailFenceCipher;

public class BouncingIndexTest {

    private BouncingIndex bouncingIndex;

    @BeforeEach
    void setUp(){
        bouncingIndex = new BouncingIndex(2);
    }


    @Test
    public void nextTestOk(){
        //Arrange
        Integer esperado = 0;
        //Act
        Integer resultado = bouncingIndex.next();
        //Assert
        Assert.assertEquals(esperado, resultado);
    }

    @Test
    public void nextTestWithNegativeIndexOk(){
        //Arrange
        BouncingIndex bouncingIndex2 = new BouncingIndex(-10);
        Integer esperado = 0;
        //Act
        Integer resultado = bouncingIndex.next();
        //Assert
        Assert.assertEquals(esperado, resultado);
    }
}