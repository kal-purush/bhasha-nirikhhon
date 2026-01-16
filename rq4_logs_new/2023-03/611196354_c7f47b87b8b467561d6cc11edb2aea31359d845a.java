package ro.ase.cts;

import ro.ase.cts.contract.DataBase;
import ro.ase.cts.sql.SQL;

import java.lang.reflect.InvocationTargetException;

public class Main {

    public static void main(String[] args) {
        try {
            DataBase database = (SQL) Class.forName("ro.ase.cts.sql.SQL").getDeclaredConstructor().newInstance();
            database.database();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}