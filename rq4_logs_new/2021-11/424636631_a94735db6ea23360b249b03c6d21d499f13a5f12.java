package com.com619.group6.controllers;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.com619.group6.model.PortService;

/**
 * Rest controller for Port Service Orders (PSOs)
 * 
 * @author WhitearL
 *
 */
@RestController
public class PortServiceController {

	/**
	 * Create a new PSO
	 * 
	 * @return Boolean indicating success.
	 */
	@GetMapping(value = "/createPSO")
	public boolean createPSO(/* TODO: Params? */) {

		return true;
	}

	/**
	 * Edit an existing PSO
	 * 
	 * @param psoID the ID of the PSO
	 * 
	 * @return Boolean indicating success.
	 */
	@GetMapping(value = "/editPSO")
	public boolean editPSO(@RequestParam(required = true) int psoID /* TODO: Params? */) {

		return true;
	}

	/**
	 * Cancel an existing PSO
	 * 
	 * @param psoID the ID of the PSO
	 * 
	 * @return Boolean indicating success.
	 */
	@GetMapping(value = "/cancelPSO")
	public boolean cancelPSO(@RequestParam(required = true) int psoID) {

		return true;
	}

	/**
	 * Mark an existing PSO as complete and close it.
	 * 
	 * @param psoID the ID of the PSO
	 * 
	 * @return Boolean indicating success.
	 */
	@GetMapping(value = "/completePSO")
	public boolean completePSO(@RequestParam(required = true) int psoID) {

		return true;
	}

	/**
	 * Calculate the total cost of a PSO
	 * 
	 * @param psoID the ID of the PSO
	 * 
	 * @return The total cost of the PSO with the ID.
	 */
	@GetMapping(value = "/calculatePSOCost")
	public double calculatePSOCost(@RequestParam(required = true) int psoID) {

		return 1;
	}

	/**
	 * Assign a stevedore to a PSO
	 * 
	 * @param psoID       ID of PSO to assign to
	 * @param stevedoreID ID of stevedore to assign
	 * @return Boolean indicating success
	 */
	@GetMapping(value = "/assignStevedore")
	public boolean assignStevedore(@RequestParam(required = true) int psoID,
			@RequestParam(required = true) int stevedoreID) {

		return true;
	}

	/**
	 * Unassign a stevedore from a PSO
	 * 
	 * @param psoID       ID of PSO to unassign from
	 * @param stevedoreID ID of stevedore to unassign
	 * @return Boolean indicating success
	 */
	@GetMapping(value = "/unassignStevedore")
	public boolean unassignStevedore(@RequestParam(required = true) int psoID,
			@RequestParam(required = true) int stevedoreID) {

		return true;
	}

	/**
	 * Return a list of services comprising the PSO with the ID
	 * 
	 * @param psoID ID of PSO to get the services from.
	 * @return List of services required for a given PSO.
	 */
	@GetMapping(value = "/getServicesRequired")
	public List<PortService> getServicesRequired(@RequestParam(required = true) int psoID) {

		return new ArrayList<>();
	}

	/**
	 * Complete a service as part of a PSO
	 * 
	 * @param psoID     ID of PSO to complete a service on.
	 * @param serviceID ID of service to complete.
	 * @return Boolean indicating success.
	 */
	@GetMapping(value = "/completeService")
	public boolean completeService(@RequestParam(required = true) int psoID,
			@RequestParam(required = true) int serviceID) {

		return true;

	}

}