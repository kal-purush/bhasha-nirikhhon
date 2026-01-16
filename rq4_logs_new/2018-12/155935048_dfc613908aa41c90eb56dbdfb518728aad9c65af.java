package br.com.casadocodigo.loja.controllers.test;

import br.com.casadocodigo.loja.controllers.HomeController;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HomeControllerTest {

    private HomeController subject;

    @Before
    public void before() {
        subject = new HomeController();
    }

    @Test
    public void index() {
        assertEquals("home", subject.index());
    }
}