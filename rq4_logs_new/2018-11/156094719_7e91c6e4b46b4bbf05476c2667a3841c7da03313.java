
package com.kadirkorkmaz.react.repository;

import com.kadirkorkmaz.react.entity.Meeting;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.web.bind.annotation.CrossOrigin;

@CrossOrigin
@RepositoryRestResource(collectionResourceRel = "meeting", path = "meeting")
public interface MeetingRepository extends JpaRepository<Meeting, Long> {

}