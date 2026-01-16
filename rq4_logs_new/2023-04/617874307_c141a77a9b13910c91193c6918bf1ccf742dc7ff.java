
package acme.features.auditor.auditingRecords;

import java.util.Collection;

import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import acme.entities.audit.AuditingRecords;
import acme.framework.repositories.AbstractRepository;
import acme.roles.Auditor;

@Repository
public interface AuditorAuditingRecordRepository extends AbstractRepository {

	@Query("SELECT ar FROM AuditingRecords ar WHERE ar.audit.id = :id")
	Collection<AuditingRecords> findAllAuditingRecordsFromAudit(int id);

	@Query("SELECT ar FROM AuditingRecords ar WHERE ar.id= :id")
	AuditingRecords findAuditingRecordById(int id);

	@Query("SELECT a FROM Auditor a WHERE a.id= :id")
	Auditor findAuditorById(int id);

}