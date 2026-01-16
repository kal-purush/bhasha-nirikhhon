package com.ravi.cache.statistics.delegator;

import com.ravi.cache.statistics.dto.TeacherDTO;
import com.ravi.cache.statistics.entity.Teacher;
import com.ravi.cache.statistics.manager.TeacherServiceManager;
import com.ravi.cache.statistics.mapper.TeacherDataMapper;

import java.util.List;

public class TeacherDelegator {

    private final TeacherServiceManager teacherServiceManager;
    private final TeacherDataMapper teacherDataMapper;

    public TeacherDelegator(TeacherServiceManager teacherServiceManager, TeacherDataMapper teacherDataMapper) {
        this.teacherServiceManager = teacherServiceManager;
        this.teacherDataMapper = teacherDataMapper;
    }

    public List<TeacherDTO> getAllTeachers() {
        return teacherDataMapper.mapToTeacherDTOList(teacherServiceManager.getAllTeachers());
    }

    public TeacherDTO getTeacherById(Long id) {
        return teacherDataMapper.mapToTeacherDTO(teacherServiceManager.getTeacherById(id));
    }

    public TeacherDTO createTeacher(TeacherDTO teacherDTO) {
        Teacher teacher = teacherDataMapper.mapToTeacher(teacherDTO);
        return teacherDataMapper.mapToTeacherDTO(teacherServiceManager.createTeacher(teacher));
    }

    public TeacherDTO updateTeacher(TeacherDTO teacherDTO) {
        Teacher teacher = teacherDataMapper.mapToTeacher(teacherDTO);
        return teacherDataMapper.mapToTeacherDTO(teacherServiceManager.updateTeacher(teacher));
    }

    public void deleteTeacherById(Long id) {
        teacherServiceManager.deleteTeacherById(id);
    }
}