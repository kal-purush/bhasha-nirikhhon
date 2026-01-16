package ru.yandex.practicum.filmorate.storage;

public class SqlRequests {
    public static final String SQL_GET_USER_BY_ID = "SELECT * FROM " + Tables.CLIENT + " WHERE id = ?";
    public static final String SQL_GET_USER_FRIEND_LIST_BY_ID = "SELECT * FROM " + Tables.FRIEND + " WHERE client_id = ?";
    public static final String SQL_INSERT_USER = "INSERT INTO " + Tables.CLIENT + "(email, login, name, birthday) VALUES (?, ?, ?, ?, )";
    public static final String SQL_UPDATE_USER = "UPDATE " + Tables.CLIENT + " SET EMAIL = ?, LOGIN = ?, NAME = ?, BIRTHDAY = ? WHERE ID = ?";
    public static final String SQL_GET_ALL_USERS = "SELECT * FROM " + Tables.CLIENT;
    public static final String SQL_DELETE_ALL_USERS = "DELETE FROM " + Tables.CLIENT;


    public enum Tables {
        CLIENT("CLIENT"), //на USER ругается БД
        FRIEND("FRIEND"),
        FILM("FILM"),
        GENRE("GENRE"),
        RATING("RATING"),
        LIKES("LIKES");

        private final String table;

        Tables(final String table) {
            this.table = table;
        }

        @Override
        public String toString() {
            return table;
        }
    }
}