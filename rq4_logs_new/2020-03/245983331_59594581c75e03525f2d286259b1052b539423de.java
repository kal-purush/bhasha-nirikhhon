package com.ipiecoles.java.java350.repository;


import com.ipiecoles.java.java350.model.Employe;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class EmployeRepositoryTest {
    @Autowired
    EmployeRepository employeRepository;
    @Test
    public void testFindLastMatricule() {
        //Given
        Employe employe1, employe2;
        employe1 = new Employe();
        employe2 = new Employe();
        employe1.setMatricule("M12345");
        employe2.setMatricule("M23456");

        employeRepository.save(employe1);
        Employe employe = employeRepository.save(employe2);

        //When
        String res = employeRepository.findLastMatricule();

        //Then
        Assertions.assertThat(res).isEqualTo(employe);
    }
}