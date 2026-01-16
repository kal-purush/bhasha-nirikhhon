package sky.pro.telegrambotforpets.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;

/**
 * создана сущность Guest, нужна для первичной коммуникации
 * подключил к таблице guests
 */
@Entity
@Table(name = "guests")
public class Guest {
    @Id
    private Long chatId;
    private String userName;

    public Guest() {
    }

    public Guest(Long chatId, String userName) {
        this.chatId = chatId;
        this.userName = userName;
    }

    public Long getChatId() {
        return chatId;
    }

    public void setChatId(Long chatId) {
        this.chatId = chatId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Guest guest = (Guest) o;
        return chatId.equals(guest.chatId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chatId);
    }

    @Override
    public String toString() {
        return "Guest{" +
                "chatId=" + chatId +
                ", userName='" + userName + '\'' +
                '}';
    }
}