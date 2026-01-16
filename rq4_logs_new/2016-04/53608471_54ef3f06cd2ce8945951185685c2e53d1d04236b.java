package pl.polsl.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.polsl.repository.ReportsRepository;
import pl.polsl.model.ReportEntity;

import javax.annotation.PostConstruct;

/**
 * Created by Kuba on 02.04.2016.
 */

@Component
public class ReportsController {

    @Autowired
    private ReportsRepository reportsRepository;

    @Transactional
    public ReportEntity findReport(int id){
        return reportsRepository.findOne(id);}
}