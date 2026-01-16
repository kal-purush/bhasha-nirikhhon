package org.rentifytools.controller;

import lombok.RequiredArgsConstructor;
import org.rentifytools.entity.Tool;
import org.rentifytools.service.ToolService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tools")
@RequiredArgsConstructor
public class ToolController {

    private final ToolService toolService;

    @GetMapping()
    public List<Tool> getAllTools() {
        return toolService.getAllTools();
    }

    @PostMapping
    public void addTool(@RequestBody Tool tool) {
        toolService.addTool(tool);
    }


}