
package acme.features.assistant.dashboard;

import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import acme.entities.enumerates.ActivityType;
import acme.framework.repositories.AbstractRepository;
import acme.roles.Assistant;

@Repository
public interface AssistantDashboardRepository extends AbstractRepository {

	@Query("select a from Assistant a where a.userAccount.id = :id")
	Assistant findOneAssistantByUserAccountId(int id);

	@Query("select count(distinct ts) from TutorialSession ts")
	Integer findNumOfAssistantSessionsBySessionType(ActivityType st, Assistant a);

}