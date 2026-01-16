package le.ac.uk.repository;

import le.ac.uk.model.Activity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ActivityRepository extends CrudRepository<Activity, Long> {

    @Query("SELECT a.name,a.type,a.slots,a.price,c.town,c.region,c.latitude,c.longitude,c.postcode,a.id from Activity a INNER JOIN City c on c.id=a.cityID where c.region=:city and a.type=:type")
    List<Object[]> findByActivityType(String city, int type);
}