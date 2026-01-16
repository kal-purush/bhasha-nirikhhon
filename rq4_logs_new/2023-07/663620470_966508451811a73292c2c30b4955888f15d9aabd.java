package pl.kurs.exchangerateapponspringboot.models;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;

@Entity
public class ExchangeEvent implements Serializable {
    public static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue
    private Long id;
    private Timestamp date;
    private String currencyFrom;
    private BigDecimal amountFrom;
    private String currencyTo;
    private BigDecimal amountTo;
    private BigDecimal exchangeRate;

    public ExchangeEvent() {
    }

    public ExchangeEvent(Timestamp date, String currencyFrom, BigDecimal amountFrom, String currencyTo, BigDecimal amountTo, BigDecimal exchangeRate) {
        this.date = date;
        this.currencyFrom = currencyFrom;
        this.amountFrom = amountFrom;
        this.currencyTo = currencyTo;
        this.amountTo = amountTo;
        this.exchangeRate = exchangeRate;
    }

    public Long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeEvent that = (ExchangeEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(date, that.date) && Objects.equals(currencyFrom, that.currencyFrom) && Objects.equals(amountFrom, that.amountFrom) && Objects.equals(currencyTo, that.currencyTo) && Objects.equals(amountTo, that.amountTo) && Objects.equals(exchangeRate, that.exchangeRate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, date, currencyFrom, amountFrom, currencyTo, amountTo, exchangeRate);
    }

    @Override
    public String toString() {
        return "ExchangeEvent{" +
                "id=" + id +
                ", date=" + date +
                ", currencyFrom='" + currencyFrom + '\'' +
                ", amountFrom=" + amountFrom +
                ", currencyTo='" + currencyTo + '\'' +
                ", amountTo=" + amountTo +
                ", exchangeRate=" + exchangeRate +
                '}';
    }
}