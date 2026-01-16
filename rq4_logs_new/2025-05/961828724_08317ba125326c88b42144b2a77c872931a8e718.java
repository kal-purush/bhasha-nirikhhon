package com.project.quanlycanghangkhong.controller;

import com.project.quanlycanghangkhong.dto.CreateTaskRequest;
import com.project.quanlycanghangkhong.dto.TaskDTO;
import com.project.quanlycanghangkhong.dto.TaskDetailDTO;
import com.project.quanlycanghangkhong.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {
    @Autowired
    private TaskService taskService;

    // Tạo công việc
    @PostMapping
    public ResponseEntity<TaskDTO> createTask(@RequestBody CreateTaskRequest request) {
        TaskDTO created = taskService.createTaskWithAssignmentsAndDocuments(request);
        return ResponseEntity.ok(created);
    }

    // Cập nhật công việc
    @PutMapping("/{id}")
    public ResponseEntity<TaskDTO> updateTask(@PathVariable Integer id, @RequestBody TaskDTO taskDTO) {
        TaskDTO updated = taskService.updateTask(id, taskDTO);
        if (updated == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(updated);
    }

    // Xóa công việc
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTask(@PathVariable Integer id) {
        taskService.deleteTask(id);
        return ResponseEntity.noContent().build();
    }

    // Lấy chi tiết công việc (bao gồm assignment, document, attachment)
    @GetMapping("/{id}")
    public ResponseEntity<TaskDetailDTO> getTaskDetailById(@PathVariable Integer id) {
        TaskDetailDTO task = taskService.getTaskDetailById(id);
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(task);
    }

    // Lấy danh sách công việc (bao gồm assignment, document, attachment)
    @GetMapping
    public ResponseEntity<List<TaskDetailDTO>> getAllTaskDetails() {
        List<TaskDetailDTO> tasks = taskService.getAllTaskDetails();
        return ResponseEntity.ok(tasks);
    }
}