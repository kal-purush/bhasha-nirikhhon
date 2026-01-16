package com.rljj.chipservice.domain.exchange.service;

import com.rljj.chipservice.domain.exchange.dto.ChipExchangeCreateRequest;
import com.rljj.chipservice.domain.exchange.dto.ChipExchangeResponse;
import com.rljj.chipservice.domain.exchange.dto.ChipExchangeStatusUpdateRequest;
import com.rljj.chipservice.domain.exchange.repository.ChipExchangeRepository;
import com.rljj.switchswitchcommon.exception.NotFoundException;
import com.rljj.switchswitchentity.chip.chipexchange.ChipExchange;
import com.rljj.switchswitchentity.chip.chipexchange.ChipExchangeStatus;
import com.rljj.switchswitchentity.chip.chipinfo.ChipInfo;
import com.rljj.switchswitchentity.chip.chippost.ChipPost;
import com.rljj.switchswitchentity.member.Member;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
@Transactional(readOnly = true)
@Service
public class ChipExchangeServiceImpl implements ChipExchangeService {

    private final ChipExchangeRepository chipExchangeRepository;

    @Override
    public Page<ChipExchangeResponse> getChipExchanges(Long chipPostId, Pageable pageable) {
        return chipExchangeRepository.findAllByChipPostId(chipPostId, pageable)
                .map(ChipExchangeResponse::from);
    }

    @Override
    @Transactional
    public void createChipExchange(UserDetails userDetails, ChipExchangeCreateRequest request) {
        ChipInfo chipInfo = ChipInfo.builder().id(request.getChipInfoId()).build();
        ChipPost chipPost = ChipPost.builder().id(request.getChipPostId()).build();
        Member member = Member.builder().id(Long.parseLong(userDetails.getUsername())).build();

        chipExchangeRepository.save(ChipExchange.builder()
                .chipPost(chipPost)
                .chipInfo(chipInfo)
                .member(member)
                .content(request.getContent())
                .status(ChipExchangeStatus.REQUESTED)
                .build());
    }

    @Override
    @Transactional
    public void updateStatus(ChipExchangeStatusUpdateRequest request) {
        ChipExchange chipExchange = getChipExchange(request.getChipExchangeId());
        chipExchange.updateStatus(request.getStatus());
    }

    @Override
    public ChipExchange getChipExchange(Long chipExchangeId) {
        return chipExchangeRepository.findById(chipExchangeId)
                .orElseThrow(() -> new NotFoundException("Not Found chip exchange id : " + chipExchangeId));
    }
}