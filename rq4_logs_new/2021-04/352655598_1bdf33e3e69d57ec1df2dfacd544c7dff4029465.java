package com.kodilla.ecommerce.controller;

import com.kodilla.ecommerce.dto.GroupDto;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/v1/group", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
public class GroupController {

    @GetMapping("/{groupId}")
    public GroupDto getGroup(@PathVariable Long groupId) {
        return GroupDto.builder()
                .groupId(100L)
                .name("Odzież")
                .build();
    }

    @GetMapping
    public List<GroupDto> getGroups() {
        return Arrays.asList(
                GroupDto.builder()
                        .groupId(100L)
                        .name("Odzież")
                        .build()
        );
    }

    @PostMapping
    public GroupDto createGroup (@RequestBody final GroupDto groupDto) {
        return GroupDto.builder()
                .groupId(100L)
                .name("Odzież - create new group")
                .build();
    }

    @PutMapping
    public GroupDto updateGroup(@RequestBody final GroupDto groupDto) {
        return GroupDto.builder()
                .groupId(100L)
                .name("Odzież - updated group")
                .build();
    }
}