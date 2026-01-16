package com.bnau.cdb.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bnau.cdb.dto.ComputerDto;
import com.bnau.cdb.service.ComputerService;
import com.bnau.cdb.swagger.ApiPageable;
import com.bnau.cdb.util.MapperUtil;

import springfox.documentation.annotations.ApiIgnore;

/**
 * Controller for operations on computer.
 *
 * @author bertrand
 *
 */
@RestController
@RequestMapping("api/computers")
public class ComputerController {
	
	@Autowired
	private ComputerService computerService;
	
	@Autowired
	private MapperUtil mapperUtil;
	
	@ApiPageable
	@GetMapping
	public Page<ComputerDto> findComputers(@ApiIgnore final Pageable pageable) {
		return mapperUtil.map(computerService.findComputers(pageable), ComputerDto.class);
	}
	
	@GetMapping("/{id}")
	public ComputerDto findComputerById(@PathVariable final Long id) {
		return mapperUtil.map(computerService.findComputerById(id), ComputerDto.class);
	}
	
	@PostMapping
	public void updateComputer(@RequestBody @Valid final ComputerDto computer) {
		computerService.updateComputer(computer);
	}
	
	@DeleteMapping("/{id}")
	public void deleteComputer(@PathVariable final Long id) {
		computerService.deleteComputer(id);
	}
	
	@PutMapping
	public Long addComputer(@RequestBody @Valid final ComputerDto computer) {
		return computerService.addComputer(computer).getId();
	}
}