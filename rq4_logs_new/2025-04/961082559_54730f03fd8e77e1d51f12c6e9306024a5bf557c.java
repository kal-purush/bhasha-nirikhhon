package ma.enset.tp2_ormjpahibernatespringdata.repository;

import ma.enset.tp2_ormjpahibernatespringdata.entities.JPAPatient;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PatientRepository extends JpaRepository<JPAPatient,Long> {

}