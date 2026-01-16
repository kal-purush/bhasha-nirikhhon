package com.cartas.jaktani.controller;

import com.cartas.jaktani.dto.DocumentDto;
import com.cartas.jaktani.exceptions.ResourceNotFoundException;
import com.cartas.jaktani.service.DocumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/api/document")
public class DocumentController {
    @Autowired
    DocumentService documentService;

    @PostMapping(path = "/findById") 
	public Object findDocumentByID(@RequestBody DocumentDto document) { 
		return documentService.findDocumentByID(document); 
	}
    
    @PostMapping(path = "/deleteByID") 
   	public Object deleteDocumentByID(@RequestBody DocumentDto document) { 
   		return documentService.deleteDocumentByID(document); 
   	}
    
    @PostMapping(path = "/add") 
   	public Object addDocument(@RequestBody DocumentDto document) { 
   		return documentService.addDocument(document); 
   	}

	@PostMapping(path = "/edit") 
	public Object editDocument(@RequestBody DocumentDto document) { 
		return documentService.editDocument(document); 
	}
	
	@PostMapping(path = "/findAllByType") 
	public Object findAllByType(@RequestBody DocumentDto document) { 
		return documentService.findAllByType(document); 
	}

	@PostMapping(path = "/findAllByRefferenceIdAndType") 
	public Object findAllByRefferenceIdAndType(@RequestBody DocumentDto document) { 
		return documentService.findAllByRefferenceIdAndType(document); 
	}
	
	@PostMapping(path = "/findAllByRefferenceIdAndTypeAndCode") 
	public Object findAllByRefferenceIdAndTypeAndCode(@RequestBody DocumentDto document) { 
		return documentService.findAllByRefferenceIdAndTypeAndCode(document); 
	}

}