package com.kodilla.ecommercee.service;

import com.kodilla.ecommercee.GroupRepository;
import com.kodilla.ecommercee.domain.Group;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class DbGroupService {
    private final GroupRepository groupRepository;

    public void createGroup(Group group) {
        groupRepository.save(group);
    }

    public List<Group> getAllGroups() {
        return groupRepository.findAll();
    }

    public Optional<Group> getGroup(final Long id) {
        return groupRepository.findById(id);
    }

    public void deleteGroup(final Long id) {
        if (groupRepository.existsById(id)) {
            groupRepository.deleteById(id);
        }
    }
}