package com.chapssal_tteok.preview.domain.resume.service;

import com.chapssal_tteok.preview.domain.resume.converter.ResumeConverter;
import com.chapssal_tteok.preview.domain.resume.dto.ResumeRequestDTO;
import com.chapssal_tteok.preview.domain.resume.entity.Resume;
import com.chapssal_tteok.preview.domain.resume.repository.ResumeRepository;
import com.chapssal_tteok.preview.domain.user.entity.Role;
import com.chapssal_tteok.preview.domain.user.entity.User;
import com.chapssal_tteok.preview.global.apiPayload.code.status.ErrorStatus;
import com.chapssal_tteok.preview.global.apiPayload.exception.handler.ResumeHandler;
import com.chapssal_tteok.preview.global.apiPayload.exception.handler.UserHandler;
import com.chapssal_tteok.preview.security.SecurityUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ResumeCommandServiceImpl implements ResumeCommandService {

    private final ResumeRepository resumeRepository;
    private final SecurityUtil securityUtil;

    @Override
    @Transactional
    public Resume createResume(ResumeRequestDTO.CreateResumeDTO request) {

        // 현재 로그인된 사용자 정보 가져오기
        User user = securityUtil.getCurrentUser();

        Resume newResume = ResumeConverter.toResume(request, user);

        return resumeRepository.save(newResume);
    }

    @Override
    @Transactional
    public Resume updateResume(ResumeRequestDTO.UpdateResumeDTO request) {

        // 현재 로그인된 사용자 정보 가져오기
        User user = securityUtil.getCurrentUser();

        Resume resume = resumeRepository.findById(request.getResumeId())
                .orElseThrow(() -> new ResumeHandler(ErrorStatus.RESUME_NOT_FOUND));

        // 자기 자신이거나 관리자 권한이 있는 경우만 허용
        if (!user.getId().equals(resume.getUser().getId()) &&
                !user.getRole().equals(Role.ADMIN)) {
            throw new UserHandler(ErrorStatus.USER_NOT_AUTHORIZED);
        }

        if (request.getTitle() != null) {
            resume.updateTitle(request.getTitle());
        }

        return resumeRepository.save(resume);
    }

    @Override
    @Transactional
    public void deleteResume(Long resumeId) {

        // 현재 로그인된 사용자 정보 가져오기
        User user = securityUtil.getCurrentUser();

        Resume resume = resumeRepository.findById(resumeId)
                .orElseThrow(() -> new ResumeHandler(ErrorStatus.RESUME_NOT_FOUND));

        // 자기 자신이거나 관리자 권한이 있는 경우만 허용
        if (!user.getId().equals(resume.getUser().getId()) &&
                !user.getRole().equals(Role.ADMIN)) {
            throw new UserHandler(ErrorStatus.USER_NOT_AUTHORIZED);
        }

        resumeRepository.delete(resume);
    }
}