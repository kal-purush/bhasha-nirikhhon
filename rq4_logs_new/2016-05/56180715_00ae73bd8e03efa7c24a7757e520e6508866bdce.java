package app.ticketing.td.movieticketingsystem.models;

import java.sql.Date;
import java.sql.Time;

public class Seat {
    private int RoomID;
    private int SeatRow;
    private int SeatColumn;
    private Date MovieDate;
    private Time MovieTime;
    private boolean IsTaken;

    public boolean isTaken() {
        return IsTaken;
    }

    public void setTaken(boolean taken) {
        IsTaken = taken;
    }

    public int getSeatRow() {
        return SeatRow;
    }

    public void setSeatRow(int seatRow) {
        SeatRow = seatRow;
    }

    public int getSeatColumn() {
        return SeatColumn;
    }

    public void setSeatColumn(int seatColumn) {
        SeatColumn = seatColumn;
    }

    public Date getMovieDate() {
        return MovieDate;
    }

    public void setMovieDate(Date movieDate) {
        MovieDate = movieDate;
    }

    public Time getMovieTime() {
        return MovieTime;
    }

    public void setMovieTime(Time movieTime) {
        MovieTime = movieTime;
    }

    public int getRoomID() {
        return RoomID;
    }

    public void setRoomID(int roomID) {
        RoomID = roomID;
    }
}