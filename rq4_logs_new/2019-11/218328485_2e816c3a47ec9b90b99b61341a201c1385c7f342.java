package com.project.teamup.mapper;


import com.project.teamup.dto.ProjectDTO;
import com.project.teamup.model.Project;
import org.mapstruct.Mapper;

import java.util.List;

@Mapper(uses = {IndustryMapper.class, CompanyMapper.class})
public abstract class ProjectMapper {
    public abstract Project toEntity(ProjectDTO dto);

    public abstract List<Project> toEntityList(List<ProjectDTO> dtoList);

    public abstract ProjectDTO toDto(Project entity);

    public abstract List<ProjectDTO> toDtoList(List<Project> entityList);
}