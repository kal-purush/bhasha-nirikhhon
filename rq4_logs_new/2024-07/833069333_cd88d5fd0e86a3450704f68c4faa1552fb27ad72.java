package business;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.assertj.core.api.Assertions.assertThat;


import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;


public class ListBDDTest {

    @Mock
    List<String> mockList;

    @BeforeEach
    public void setup(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void BDDTest(){
        //Given (Setup)
        given(mockList.get(anyInt())).willReturn("AnsyGitHub");

        //When
        String word = mockList.getFirst();

        //Then
        assertThat(word).isEqualTo("AnsyGitHub");
    }

}