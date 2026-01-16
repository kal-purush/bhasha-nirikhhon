import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AirplaneManagementUI extends JFrame {

    private JTextField searchTicketField;
    private JButton searchTicketButton;
    private JTextField searchDestinationField;
    private JTextField searchDepartureField;

    private JTextField usernameField;
    private JButton searchFlightsButton;
    private JTable flightsTable;
    private JButton viewBookingsButton;
    private JTable bookingsTable;

    private JPanel mainPanel;

    public AirplaneManagementUI() {
        super("Airplane Management System");

        establishDatabaseConnection();
        mainPanel = new JPanel(new BorderLayout());

        JPanel searchPanel = createSearchPanel();
        mainPanel.add(searchPanel, BorderLayout.NORTH);


        JPanel flightsPanel = createFlightsPanel();
        mainPanel.add(flightsPanel, BorderLayout.CENTER);

        JPanel bookingsPanel = createBookingsPanel();
        mainPanel.add(bookingsPanel, BorderLayout.SOUTH);

        setWindowProperties();
    }

    private void establishDatabaseConnection() {
        String url = JOptionPane.showInputDialog(null, "Enter database URL:");
        String user = JOptionPane.showInputDialog(null, "Enter database username:");
        String password = JOptionPane.showInputDialog(null, "Enter database password:");

        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to MySQL database");
        } catch (SQLException e) {
            System.out.println("Failed to connect to MySQL database: " + e.getMessage());
        }
    }

    private JPanel createSearchPanel() {
        JPanel searchPanel = new JPanel(new GridLayout(2, 1));
        JPanel searchTicketPanel = createSearchTicketPanel();
        JPanel searchFlightsPanel = createSearchFlightsPanel();
        searchPanel.add(searchTicketPanel);
        searchPanel.add(searchFlightsPanel);
        return searchPanel;
    }

    private JPanel createSearchTicketPanel() {
        JPanel searchTicketPanel = new JPanel(new FlowLayout());
        searchTicketField = new JTextField(10);
        searchTicketButton = new JButton("Search Ticket");
        searchTicketPanel.add(new JLabel("Ticket number: "));
        searchTicketPanel.add(searchTicketField);
        searchTicketPanel.add(searchTicketButton);
        return searchTicketPanel;
    }

    private JPanel createSearchFlightsPanel() {
        JPanel searchFlightsPanel = new JPanel(new FlowLayout());
        searchDestinationField = new JTextField(10);
        searchDepartureField = new JTextField(10);
        searchFlightsButton = new JButton("Search Flights");


        // Create JComboBoxes for selecting the search date
        JComboBox<String> searchMonthComboBox = new JComboBox<>(new String[]{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"});
        JComboBox<String> searchDayComboBox = new JComboBox<>(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"});
        JTextField searchYearTextField = new JTextField(4);
        JPanel searchDatePanel = new JPanel(new FlowLayout());
        searchDatePanel.add(new JLabel("Date: "));
        searchDatePanel.add(searchMonthComboBox);
        searchDatePanel.add(searchDayComboBox);
        searchDatePanel.add(searchYearTextField);


        searchFlightsPanel.add(new JLabel("Destination: "));
        searchFlightsPanel.add(searchDestinationField);
        searchFlightsPanel.add(new JLabel("Departure city: "));
        searchFlightsPanel.add(searchDepartureField);
        searchFlightsPanel.add(searchDatePanel);
        searchFlightsPanel.add(searchFlightsButton);

        return searchFlightsPanel;
    }


    private JPanel createSearchDatePanel() {
        // Create JComboBoxes for selecting the search date
        JComboBox<String> searchMonthComboBox = new JComboBox<>(new String[]{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"});
        JComboBox<String> searchDayComboBox = new JComboBox<>(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"});
        JComboBox<String> searchYearComboBox = new JComboBox<>(new String[]{"2022", "2023", "2024", "2025", "2026", "2027", "2028", "2029", "2030", "2031", "2032", "2033", "2034", "2035", "2036", "2037", "2038", "2039", "2040", "2041", "2042", "2043", "2044", "2045", "2046", "2047", "2048", "2049", "2050"});
        JPanel searchDatePanel = new JPanel(new FlowLayout());
        searchDatePanel.add(new JLabel("Date: "));
        searchDatePanel.add(searchMonthComboBox);
        searchDatePanel.add(searchDayComboBox);
        searchDatePanel.add(searchYearComboBox);
        return searchDatePanel;
    }

    private JPanel createFlightsPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        String[] columns = {"Flight ID", "Departure City", "Destination", "Price", "Departure Date/Time"};
        Object[][] data = {
                {1, "LAX", "JFK", 500.0, "2023-04-01 10:00:00"},
                {2, "SFO", "JFK", 600.0, "2023-04-02 12:00:00"},
                {3, "LAX", "BOS", 550.0, "2023-04-03 11:00:00"},
                {4, "JFK", "LAX", 450.0, "2023-04-04 13:00:00"},
                {5, "JFK", "SFO", 650.0, "2023-04-05 14:00:00"}
        };

        DefaultTableModel model = new DefaultTableModel(data, columns);
        flightsTable = new JTable(model);
        JScrollPane scrollPane = new JScrollPane(flightsTable);
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createBookingsPanel() {
        JPanel bookingsPanel = new JPanel(new BorderLayout());

        // Set up the bookings table
        String[] bookingsColumns = {"Booking ID", "Flight ID", "Passenger Name", "Passenger Gender", "Passenger Passport"};
        Object[][] bookingsData = {
                {1, 1, "John Smith", "M", "1234567890"},
                {2, 2, "Jane Doe", "F", "0987654321"},
                {3, 3, "Bob Johnson", "M", "4567890123"},
                {4, 4, "Alice Brown", "F", "7890123456"},
                {5, 5, "Tom Jones", "M", "3216549870"}
        };
        DefaultTableModel bookingsTableModel = new DefaultTableModel(bookingsData, bookingsColumns);
        bookingsTable = new JTable(bookingsTableModel);
        JScrollPane bookingsScrollPane = new JScrollPane(bookingsTable);
        bookingsPanel.add(bookingsScrollPane, BorderLayout.CENTER);

        // Set up the view bookings button
        JPanel viewBookingsPanel = new JPanel(new FlowLayout());
        viewBookingsButton = new JButton("View Bookings");
        usernameField = new JTextField(10);
        viewBookingsPanel.add(new JLabel("Username: "));
        viewBookingsPanel.add(usernameField);
        viewBookingsPanel.add(viewBookingsButton);
        bookingsPanel.add(viewBookingsPanel, BorderLayout.NORTH);

        return bookingsPanel;
    }

    private void setWindowProperties() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        getContentPane().add(mainPanel);
        pack();
        setVisible(true);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new AirplaneManagementUI());
    }
}