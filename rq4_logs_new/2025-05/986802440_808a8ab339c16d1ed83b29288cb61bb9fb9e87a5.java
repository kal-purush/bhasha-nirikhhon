package com.ceos.spring_vote_21st.vote.repository;

import com.ceos.spring_vote_21st.member.domain.Member;
import com.ceos.spring_vote_21st.vote.domain.Candidate;
import com.ceos.spring_vote_21st.vote.domain.Election;
import com.ceos.spring_vote_21st.vote.domain.Vote;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface VoteRepository extends JpaRepository<Vote, Long> {
    List<Vote> findAllByCandidateAndElection(Candidate candidate, Election election);

//    long countByCandidateAndElection(Candidate candidate, Election election);

    boolean existsByMemberAndElection(Member member, Election election);
}