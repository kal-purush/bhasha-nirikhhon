package ua.nure.kn.dobriak.usermanagement.gui;

import ua.nure.kn.kuchinskiy.usermanagement.User;

import javax.swing.table.AbstractTableModel;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UserTableModel extends AbstractTableModel {
    private List users = null;
    private static final String[] COLUMN_NAMES = {"ID", "First Name", "Last Name", "Date of Birth"};
    private static final Class[] COLUMN_CLASSES = {Integer.class, String.class, String.class, LocalDate.class};


    public UserTableModel(Collection users) {
        this.users = new ArrayList(users);
    }

    @Override
    public int getRowCount() {
        return users.size();
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    @Override
    public Class getColumnClass(int columnIndex) {
        return COLUMN_CLASSES[columnIndex];
    }

    @Override
    public String getColumnName(int columnIndex) {
        return COLUMN_NAMES[columnIndex];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        User user = (User) users.get(rowIndex);
        switch (columnIndex) {
            case 0:
                return user.getId();
            case 1:
                return user.getFirstName();
            case 2:
                return user.getLastName();
            case 3:
                return user.getDateOfBirth();
        }
        return null;
    }
}