package example.spring.repository;

import example.spring.model.Account;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AccountsCrudRepository extends CrudRepository<Account,Long> {
    Account save(Account account);

    List<Account> findAll();

    Optional<Account> findById(long id);

    boolean existsById(long id);

    Account add(Account account);

    Account update(long id, Account account);

    void deleteById(long id);
}