package myApp.service;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "Tickets")
public class Ticket {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "user_id", insertable = false, updatable = false)
    private int userId;

    @Column(name = "creation_date")
    private LocalDateTime creationDate;

    @Column(name = "ticket_type")
    private String ticketType;

    @ManyToOne
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    public Ticket() {}

    // Getters and Setters
    public int getId() {return id;}

    public void setId(int id) {this.id = id;}

    public int getUserId() {return userId;}

    public void setUserId(int userId) {this.userId = userId;}

    public LocalDateTime getCreationDate() {return creationDate;}

    public void setCreationDate(LocalDateTime creationDate) {this.creationDate = creationDate;}

    public String getTicketType() {return ticketType;}

    public void setTicketType(String ticketType) {this.ticketType = ticketType;}

    public User getUser() {return user;}

    public void setUser(User user) {this.user = user;}
}