package com.buildnow.springbootapp.buildnowspringboot.service.demo;

import com.buildnow.springbootapp.buildnowspringboot.entitiy.Applier;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.application.Application;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.application.ApplicationEvaluation;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.application.TempPrerequisite;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.applierInfo.ExtraValue;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.applierInfo.Finance;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.applierInfo.License;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.recruitment.Grading;
import com.buildnow.springbootapp.buildnowspringboot.entitiy.recruitment.Recruitment;
import com.buildnow.springbootapp.buildnowspringboot.repository.*;
import com.buildnow.springbootapp.buildnowspringboot.repository.tempSave.TempPrerequisiteRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class DemoApplierService {
    private final ApplierRepository applierRepository;
    private final ApplicationRepository applicationRepository;
    private final RecruitmentRepository recruitmentRepository;
    private final GradingRepository gradingRepository;
    private final TempPrerequisiteRepository tempPrerequisiteRepository;
    private final ApplicationEvaluationRepository applicationEvaluationRepository;
    @Transactional
    public void duplicateAppliers(Long recruitmentId){
        Recruitment recruitment = recruitmentRepository.findById(2L)
                .orElseThrow(() -> new RuntimeException("recruitment가 없습니다."));
        Recruitment newRecruitment = recruitmentRepository.findById(recruitmentId)
                .orElseThrow(()->new RuntimeException("해당되는 recruitment가 없습니다."));
        if(!newRecruitment.getApplicationList().isEmpty()){
            throw new RuntimeException("비어있는 recruitment가 아닙니다. 새롭게 만들어낸 recruitment인지 확인하십시오.");
        }
        List<Grading> gradingList = recruitment.getGradingList();

        for(Grading grade : gradingList){
            Grading newGrading = Grading.builder()
                    .upperCategoryENUM(grade.getUpperCategoryENUM())
                    .perfectScore(grade.getPerfectScore())
                    .category(grade.getCategory())
                    .build();
            newRecruitment.addGrading(grade);
            gradingRepository.save(newGrading);
        }

        List<Application> applicationList = recruitment.getApplicationList();
        if(applicationList.isEmpty()) throw new RuntimeException("복사할 어플리케이션이 없습니다.");
        for(Application application : applicationList){
            Applier applier = application.getApplier();
            List<TempPrerequisite> tempPrerequisiteList = application.getTempPrerequisiteList();

            Application newApplication = Application.builder()
                    .applier(applier)
                    .recruitment(newRecruitment)
                    .build();

            for (TempPrerequisite tempPrerequisite : tempPrerequisiteList) {
                TempPrerequisite newTempPrerequisite = TempPrerequisite.builder()
                        .isPrerequisite(tempPrerequisite.getIsPrerequisite())
                        .prerequisiteName(tempPrerequisite.getPrerequisiteName())
                        .whyMidal(tempPrerequisite.getWhyMidal())
                        .build();
                newApplication.addTempPrerequisite(newTempPrerequisite);
                tempPrerequisiteRepository.save(newTempPrerequisite);
            }

            List<ApplicationEvaluation> applicationEvaluationList = application.getApplicationEvaluationList();
            for(ApplicationEvaluation evaluation : applicationEvaluationList){
                ApplicationEvaluation newApplicationEvaluation = ApplicationEvaluation.builder()
                        .score(evaluation.getScore())
                        .build();
                newApplicationEvaluation.setApplication(newApplication);
                Grading tempGrading = gradingRepository.findByRecruitmentAndCategory(newRecruitment, evaluation.getGrading().getCategory());
                if(tempGrading == null) throw new RuntimeException("해당하는 grading 엔티티가 없습니다.");
                newApplicationEvaluation.setGrading(tempGrading);
                applicationEvaluationRepository.save(newApplicationEvaluation);
            }
            newApplication.updateApplicationFromTempSaved(application.getWorkTypeApplying());
            newApplication.updateIsAdminTrue();
            applicationRepository.save(newApplication);
        }

    }

}