package application.model;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class RowTest {

    @Test
    public void createsVerbRow() {
        Row row = new Row("machen", Row.TYPE.VERB);

        assertThat(row.getPrimaryElement()).isEqualTo("machen");
        assertThat(row.getTypeOfRecord()).isEqualTo(Row.TYPE.VERB);
    }

    @Test
    public void createsNounRow() {
        Row row = new Row("Gabel", Row.TYPE.NOUN);

        assertThat(row.getPrimaryElement()).isEqualTo("Gabel");
        assertThat(row.getTypeOfRecord()).isEqualTo(Row.TYPE.NOUN);
    }
}