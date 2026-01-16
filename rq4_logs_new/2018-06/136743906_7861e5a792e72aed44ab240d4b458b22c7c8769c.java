package com.seachangesimulations.scorp.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import com.seachangesimulations.scorp.domain.ActorPhaseAssignment;

/** DAO Interface for ActorPhaseAssignment
 * JpaRepository - Allows generic versions of standard CRUD database ops to be used.
 * Hence no crud operations are needed in this interface or an implementing class.
 * JpaSpecificationExecutor - Allows criteria searches.
 */
@Repository  // Class is a DAO (Spring Anno)
public interface ActorPhaseAssignmentRepository extends 
	JpaRepository<ActorPhaseAssignment, Long>, JpaSpecificationExecutor<ActorPhaseAssignment> {

}