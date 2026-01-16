import com.example.Cat;
import com.example.Feline;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.Mockito;
import java.util.List;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class CatTest {

    @Mock
    private Feline felineMock;

    @Test
    public void getSoundTest() {
        // Мок объекта Feline
        Feline felineMock = mock(Feline.class);

        // Создание экземпляра Cat с использованием мока Feline
        Cat cat = new Cat(felineMock);

        // Проверка правильности возвращаемого значения метода getSound()
        Assert.assertEquals("Мяу", cat.getSound());
    }

    @Test
    public void getFoodTest() throws Exception {
        // Инициализация тестируемого объекта Cat с моком Feline
        Cat cat = new Cat(felineMock);

        // Конфигурация поведения имитации для метода eatMeat()
        Mockito.when(felineMock.eatMeat()).thenReturn(List.of("Хищник"));

        // Проверка результата
        Assert.assertEquals(List.of("Хищник"), cat.getFood());

        // Подтверждение вызова метода eatMeat() на моке
        Mockito.verify(felineMock, Mockito.times(1)).eatMeat();
    }
}