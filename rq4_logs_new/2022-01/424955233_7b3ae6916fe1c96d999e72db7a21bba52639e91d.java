package ch.volleymanager.service;

import ch.volleymanager.domain.Person;
import ch.volleymanager.exception.ParentNotDeletable;
import ch.volleymanager.exception.UserNotFoundException;
import ch.volleymanager.repo.PersonRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.List;

@Service
@Transactional

public class ParentService {
    private final ParentRepo parentRepo;


    public void deletePerson(Long id) throws ParentNotDeletable {

        // if id == parent && parent.hasPlaer
        // ndo not delete
        //else
        // delete
        /*
        Parent parent = parentRepo.findParent(id)
         */
        parentRepo.deletePersonById(id)
                .orElseThrow(() -> new ParentNotDeletable("Die Person mit der ID " + id + "kann nicht gelöscht werden."));
    }
}

}