package pie.tomato.tomatomarket.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import pie.tomato.tomatomarket.domain.Chatroom;

public interface ChatroomRepository extends JpaRepository<Chatroom, Long> {

	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("delete from Chatroom chatroom where chatroom.item.id = :itemId")
	void deleteAllByItemId(@Param("itemId") Long itemId);
}