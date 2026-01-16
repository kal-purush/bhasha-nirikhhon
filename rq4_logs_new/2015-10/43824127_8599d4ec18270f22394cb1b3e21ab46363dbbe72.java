package Interfaces;

import Entities.Students;

import java.util.List;
import java.util.Objects;

/**
 * Created by monicaramirez on 8/10/15.
 */
public interface IStudent {

    boolean saveStudent( Students students );
    boolean deleteStudent( int row );
    List<Students> listStudents();
}