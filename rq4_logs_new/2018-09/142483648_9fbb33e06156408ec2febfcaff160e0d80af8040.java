package io.testrex.controller;

import io.testrex.model.Project;
import io.testrex.repository.ProjectRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RestController
@RequestMapping(value = "/projects")
public class ProjectController {

    ProjectRepository projectRepository;
    Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    public ProjectController(ProjectRepository projectRepository) {
        this.projectRepository = projectRepository;
    }

    @GetMapping(path = "/", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public List<ResponseEntity<Project>> findAll() {
        List<ResponseEntity<Project>> resources = new ArrayList<>();
        Iterator<Project> projectIterator = projectRepository.findAll().iterator();
        while (projectIterator.hasNext()) {
            resources.add(ResponseEntity.ok(projectIterator.next()));
        }
        return resources;
    }

    @GetMapping(path = "/{id}", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Project> findById(@PathVariable Long id) {
        return projectRepository.findById(id).map(project -> ResponseEntity.ok(project)).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping(consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Project> create(@RequestBody Project project) {
        LOG.info("Creating Project: ", project.toString());
        Project responseProject = projectRepository.save(project);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(responseProject.getProjectId()).toUri();
        return ResponseEntity.created(location).body(responseProject);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        LOG.info("Deleting Project with id: " + id);
        projectRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping(path = "/{id}", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Project> put(@PathVariable Long id, @RequestBody Project newProject) {
        URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(id).toUri();
        Project requestedProject = projectRepository.findById(id).get();
        if (requestedProject != null) {
            newProject.setProjectId(id);
            LOG.info("Replacing Project on id: " + id);
            return ResponseEntity.created(location).body(projectRepository.save(newProject));
        } else {
            return ResponseEntity.badRequest().build();
        }
    }

}