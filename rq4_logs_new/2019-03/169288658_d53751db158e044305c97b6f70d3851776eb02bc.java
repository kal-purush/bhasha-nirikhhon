package com.ttn.sling.project.core.service;


import java.util.List;

public interface ClassConfigurationService {

    //int PASS_MARKS=0;

    boolean isClassLimitReached(List<Student> list);
    int getPassingMarks(ClassConfigurations classConfigurations);

}