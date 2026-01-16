package com.intouristing.service;

import com.intouristing.model.entity.Relationship;
import com.intouristing.model.entity.User;
import com.intouristing.model.enumeration.RelationshipTypeEnum;
import com.intouristing.model.key.RelationshipId;
import com.intouristing.repository.RelationshipRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Created by Marcelo Lacroix on 28/08/19.
 */
@Slf4j
@Service
public class RelationshipService extends RootService {

    private final RelationshipRepository relationshipRepository;

    public RelationshipService(RelationshipRepository relationshipRepository) {
        this.relationshipRepository = relationshipRepository;
    }

    public Relationship createFriendship(User firstUser, User secondUser) {
        RelationshipId relationshipId = this.createRelationshipId(firstUser, secondUser);
        Relationship relationship = Relationship
                .builder()
                .firstUser(relationshipId.getFirstUser())
                .secondUser(relationshipId.getSecondUser())
                .createdAt(LocalDateTime.now())
                .type(RelationshipTypeEnum.FRIENDSHIP)
                .build();

        return relationshipRepository.save(relationship);
    }

    RelationshipId createRelationshipId(User firstUser, User secondUser) {
        return (firstUser.getId() < secondUser.getId()) ? new RelationshipId(firstUser, secondUser) : new RelationshipId(secondUser, firstUser);
    }
}