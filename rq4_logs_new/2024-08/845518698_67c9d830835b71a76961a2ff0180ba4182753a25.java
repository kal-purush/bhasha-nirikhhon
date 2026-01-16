package myApp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static myApp.service.Constants.FILE_PATH;

public class BusTicket {

    private String ticketClass;
    private String ticketType;
    private String startDate;
    private int price;

    public BusTicket() {}

    public BusTicket(String ticketClass, String ticketType, String startDate, int price) {
        this.ticketClass = ticketClass;
        this.ticketType = ticketType;
        this.startDate = startDate;
        this.price = price;
    }

    // Getters and Setters
    public String getTicketClass() {return ticketClass;}

    public void setTicketClass(String ticketClass) {this.ticketClass = ticketClass;}

    public String getTicketType() {return ticketType;}

    public void setTicketType(String ticketType) {this.ticketType = ticketType;}

    public String getStartDate() {return startDate;}

    public void setStartDate(String startDate) {this.startDate = startDate;}

    public int getPrice() {return price;}

    public void setPrice(int price) {this.price = price;}

    public static List<BusTicket> getTicketsFromFile() {
        List<String> fileLines;
        List<BusTicket> ticketsList = new ArrayList<>();

        try {
            fileLines = Files.readAllLines(Paths.get(FILE_PATH));

            for (String line : fileLines) {
                try {
                    BusTicket busTicket = new ObjectMapper().readValue(line, BusTicket.class);
                    ticketsList.add(busTicket);
                } catch (JsonProcessingException e) {
                    System.err.println("Error processing JSON: " + e.getMessage());
                    return null;
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("Unexpected error reading file: " + e.getMessage());
            return null;
        }

        return ticketsList;
    }

    @Override
    public String toString() {
        return "BusTicket{ticketClass=" + ticketClass + ", ticketType='" + ticketType +
                "', startDate=" + startDate + ", price=" + price +"}";
    }
}