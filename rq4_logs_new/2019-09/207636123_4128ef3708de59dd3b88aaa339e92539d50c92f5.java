
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author castrof
 */
@Slf4j
public class VarTest {

    @Test
    public void testVar() {
        var firstName = "Frank";
        log.info("firstName={}", firstName);
    }

}