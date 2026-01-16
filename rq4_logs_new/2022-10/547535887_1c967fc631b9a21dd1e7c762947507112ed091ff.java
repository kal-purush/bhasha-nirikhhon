import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import roo2.*;

public class CharRingTest {
    private CharRing keyword;
    private CharRing keyword2;
    private CharRing keyword3;
    private CharRing keyword4;
    private CharRing keyword5;
    private char car;

@BeforeEach
    public void setupTest(){
        this.keyword = new CharRing("orientacionAobjetos");
        this.keyword2 = new CharRing("123456789");
        this.keyword3 = new CharRing("!@#$%^&*");
        this.keyword4 = new CharRing("");
    }

@Test
    public void correctValueTest(){
        setupTest();
        for (int i=0; i<=11;i++){
            this.car = this.keyword.next();
        }
        assertEquals('A',this.car);
    }
@Test
    public void numbers() {
        setupTest();
        for (int i = 0; i <= 5; i++) {
            this.car = this.keyword2.next();
        }
        assertEquals('6', this.car);
    }
@Test
    public void specialCharacters(){
        setupTest();
        for (int i=0; i<=5;i++){
            this.car = this.keyword3.next();
        }
        assertEquals('^',this.car);
    }

@Test
    public void aSingleLetter(){
        this.keyword = new CharRing("a");
            for (int i=0; i<=15;i++){
                this.car = this.keyword.next();
    }
        assertEquals('a', this.car);
}

@Test
    public void emptyString() {
        setupTest();
        /*
        Aca se rompe por el indice, cuando retorna hace idx++ y rompe
         */
        this.car = this.keyword4.next();
        assertEquals("", this.car);
    }

@Test
    public void nullValue(){
        /*aca se rompe en el null en el new cuando en el constructor hace
        source = new char[srcString.length()], no puede hacer el length
         */
        this.keyword5 = new CharRing(null);
        this.car = this.keyword5.next();
        //assertEquals(null,this.car);
    }

}