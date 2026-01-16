package com.jgp.bmo.api;

import com.jgp.bmo.dto.BMOClientDto;
import com.jgp.bmo.dto.BMOParticipantSearchCriteria;
import com.jgp.bmo.dto.MentorshipResponseDto;
import com.jgp.bmo.dto.MentorshipSearchCriteria;
import com.jgp.bmo.service.BMOClientDataService;
import com.jgp.bmo.service.MentorshipService;
import com.jgp.shared.dto.ApiResponseDto;
import com.jgp.util.CommonUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1/mentorship")
public class MentorshipController {

    private final MentorshipService mentorshipService;

    @GetMapping
    public ResponseEntity<Page<MentorshipResponseDto>> getMentorshipDataRecords(@RequestParam(name = "partnerId", required = false) Long partnerId,
                                                                                  @RequestParam(name = "participantId", required = false) Long participantId,
                                                                                  @RequestParam(name = "approvedByPartner", required = false) Boolean approvedByPartner,
                                                                                  @RequestParam(name = "pageNumber", defaultValue = "0") Integer pageNumber,
                                                                                  @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize){
        final var sortedByDateCreated =
                PageRequest.of(pageNumber, pageSize, Sort.by("dateCreated").descending());
        return new ResponseEntity<>(this.mentorshipService.getMentorshipDataRecords(new MentorshipSearchCriteria(partnerId, participantId, approvedByPartner), sortedByDateCreated), HttpStatus.OK);
    }

    @GetMapping("{mentorshipId}")
    public ResponseEntity<MentorshipResponseDto> findMentorshipDataById(@PathVariable("mentorshipId") Long mentorshipId){
        return new ResponseEntity<>(this.mentorshipService.findMentorshipDataById(mentorshipId), HttpStatus.OK);
    }

    @PostMapping("approve-or-reject")
    public ResponseEntity<ApiResponseDto> approvedMentorShipData(@RequestBody List<Long> loanIds, @RequestParam(name = "approved") Boolean approved) {
        this.mentorshipService.approvedMentorShipData(loanIds, approved);
        return new ResponseEntity<>(new ApiResponseDto(true, CommonUtil.RESOURCE_UPDATED), HttpStatus.OK);
    }
}