package works.bill.entities;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import java.time.LocalDate;
import java.util.EnumSet;

/**
 * Created by Bill on 20/03/2016.
 */
public class DatedThing {

    public DatedThing(String text, LocalDate date, EnumSet<MyEnum> myEnumSet) {
        this.text = text;
        this.date = date;
        this.myEnumSet = myEnumSet;
    }

    @javax.persistence.Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private long Id;

    private String text;

    private LocalDate date;

    private MyEnum myEnum;

    private EnumSet<MyEnum> myEnumSet;

    public long getId() {
        return Id;
    }

    public void setId(long id) {
        Id = id;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public MyEnum getMyEnum() {
        return myEnum;
    }

    public void setMyEnum(MyEnum myEnum) {
        this.myEnum = myEnum;
    }

    public EnumSet<MyEnum> getMyEnumSet() {
        return myEnumSet;
    }

    public String getText() {
        return text;
    }
}