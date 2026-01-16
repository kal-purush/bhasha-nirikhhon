package com.ceos.spring_vote_21st.vote.service;

import com.ceos.spring_vote_21st.global.error.CustomException;
import com.ceos.spring_vote_21st.global.error.ErrorCode;
import com.ceos.spring_vote_21st.member.domain.Member;
import com.ceos.spring_vote_21st.member.repository.MemberRepository;
import com.ceos.spring_vote_21st.vote.domain.*;
import com.ceos.spring_vote_21st.vote.repository.ElectionRepository;
import com.ceos.spring_vote_21st.vote.repository.VoteRepository;
import com.ceos.spring_vote_21st.vote.web.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Transactional(readOnly = true)
@Service
public class ElectionService {
    private final MemberRepository memberRepository;

    private final ElectionRepository electionRepository;

    private final VoteRepository voteRepository;
    /**
     * 선거 생성: candidates 목록까지 함께 저장됩니다.
     */
    @Transactional
    public Long createElection(ElectionCreateRequestDTO dto) {
        Election election = Election.builder()
                .name(dto.getName())
                .electionStatus(dto.getElectionStatus())
                .build();

        // cascade = ALL 이므로, Election에 붙여 두면 save() 시 함께 persist 됩니다.
        dto.getCandidates().forEach(candidateDTO -> {
            Candidate c = Candidate.create(election, candidateDTO.getName(), candidateDTO.getTeam());
            election.getCandidates().add(c);
        });


        return electionRepository.save(election).getId();
    }

    /**
     * 단건 조회
     */
    public ElectionResponseDTO getElection(Long id) {
        return electionRepository.findById(id)
                .map(ElectionResponseDTO::from)
                .orElseThrow(() -> new CustomException(ErrorCode.ENTITY_NOT_EXISTS));
    }

    /**
     * 전체 조회
     */
    public List<ElectionResponseDTO> getAllElections() {
        return electionRepository.findAll()
                .stream()
                .map(ElectionResponseDTO::from)
                .collect(Collectors.toList());
    }
/*

    */
/**
     * 업데이트: 후보 목록을 새로 바꾸면
     * orphanRemoval = true 에 의해 삭제/추가가 자동으로 처리됩니다.
     *//*

    @Transactional
    public ElectionResponseDTO updateElection(Long id, ElectionRequestDTO dto) {
        Election election = electionRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("해당 선거가 없습니다. id=" + id));

        election.setName(dto.getName());
        election.setElectionStatus(dto.getElectionStatus());
        election.setStartedAt(dto.getStartedAt());
        election.setFinishedAt(dto.getFinishedAt());

        // 후보 목록 교체
        election.getCandidates().clear();
        dto.getCandidates().forEach(cReq -> {
            Candidate c = Candidate.builder()
                    .name(cReq.getName())
                    .election(election)
                    .build();
            election.getCandidates().add(c);
        });

        Election updated = electionRepository.save(election);
        return from(updated);
    }
*/

    /**
     * 삭제: cascade + orphanRemoval 옵션으로 Vote·Candidate 모두 함께 삭제
     */
    @Transactional
    public void deleteElection(Long id) {
        if (!electionRepository.existsById(id)) {
            throw new CustomException(ErrorCode.ENTITY_NOT_EXISTS);
        }
        electionRepository.deleteById(id);
    }


    /**
     * Candidate CRUD
     */

    //create
    @Transactional
    public Long addCandidate(CandidateCreateRequestDTO dto) {
        //find election
        Election findElection = electionRepository.findById(dto.getElectionId())
                .orElseThrow(() -> new CustomException(ErrorCode.MEMBER_NOT_EXISTS));

        Candidate candidate = Candidate.create(findElection, dto.getName(),dto.getTeam());

        //save
        electionRepository.save(findElection);

        return candidate.getId();
    }


    //read
    public List<CandidateResponseDTO> getAllCandidatesByElection(Long electionId) {
        Election findElection = electionRepository.findById(electionId)
                .orElseThrow(()->new CustomException(ErrorCode.ENTITY_NOT_EXISTS));

        return findElection.getCandidates().stream()
                .map(CandidateResponseDTO::from)
                .collect(Collectors.toList());
    }

    public List<CandidateWithVoteResponseDTO> getAllCandidatesByElectionOrderByVoteCount(Long electionId) {
        Election findElection = electionRepository.findById(electionId)
                .orElseThrow(()->new CustomException(ErrorCode.ENTITY_NOT_EXISTS));

        List<CandidateWithVoteResponseDTO> dtos = findElection.getCandidates().stream()
                .map(candidate ->
                {
                    int voteCount = voteRepository.findAllByCandidateAndElection(candidate, findElection).size();
                    return CandidateWithVoteResponseDTO.from(candidate, voteCount);
                })
                .sorted((a, b) -> -(a.getVoteCount() - b.getVoteCount()))   //내림차순
                .collect(Collectors.toList());

        return dtos;
    }

    //update
    @Transactional
    public Long modifyCandidate(CandidateModifyRequestDTO dto) {
        //find election
        Election findElection = electionRepository.findById(dto.getElectionId())
                .orElseThrow(() -> new CustomException(ErrorCode.MEMBER_NOT_EXISTS));

        List<Candidate> candidates = findElection.getCandidates();

        //find candidate
        Candidate findCandidate = candidates.stream()
                .filter(candidate -> candidate.getId().equals(dto.getCandidateId()))
                .findFirst()
                .orElseThrow(() -> new CustomException(ErrorCode.ENTITY_NOT_EXISTS));

        findCandidate.update(dto);

        electionRepository.save(findElection);
        return findCandidate.getId();
    }



}