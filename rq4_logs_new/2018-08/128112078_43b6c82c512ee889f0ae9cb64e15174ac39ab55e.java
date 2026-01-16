package application.algorithm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import application.model.Row;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AlgorithmExecutorTest {

  private static final Row verbRow1 = new Row("machen", Row.TYPE.VERB);
  private static final Row verbRow2 = new Row("versehen", Row.TYPE.VERB);
  private static final Row verbRow3 = new Row("erlauben", Row.TYPE.VERB);

  private static final Row nounRow1 = new Row("Haus", Row.TYPE.NOUN);
  private static final Row nounRow2 = new Row("Hund", Row.TYPE.NOUN);
  private static final Row nounRow3 = new Row("Gabel", Row.TYPE.NOUN);

  private static final List<Row> verbRows = ImmutableList.of(verbRow1, verbRow2, verbRow3);

  @Mock
  AlgorithmSettings algorithmSettings;

  private AlgorithmExecutor algorithmExecutor;

  @Before
  public void setUp() {
    when(algorithmSettings.getAlgorithm()).thenReturn(Algorithm.NEWEST);
    algorithmExecutor = new AlgorithmExecutor(algorithmSettings);
  }

  @Test
  public void shouldMakeNewestItemsMoreImportant() {
    List<Row> actual = algorithmExecutor.apply(verbRows);

    assertThat(actual.size()).isEqualTo(6);
    assertThat(actual
        .stream()
        .filter((verbRow -> "machen".equals(verbRow.getPrimaryElement())))
        .count())
        .isEqualTo(1L);
    assertThat(actual
        .stream()
        .filter((verbRow -> "versehen".equals(verbRow.getPrimaryElement())))
        .count())
        .isEqualTo(2L);
    assertThat(actual
        .stream()
        .filter((verbRow -> "erlauben".equals(verbRow.getPrimaryElement())))
        .count())
        .isEqualTo(3L);
  }
}