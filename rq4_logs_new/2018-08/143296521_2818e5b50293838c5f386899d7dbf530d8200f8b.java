package com.spring.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.spring.model.Ticket;
import com.spring.utils.QueryBuilder;

public class TicketRepositoryImpl1 implements ITicketRepository {

	// you write code here
	@Autowired
	JdbcTemplate jdbcTemplate;

	@Override
	public int createTicket(Ticket ticket) {

		return jdbcTemplate.update(QueryBuilder.INSERT, ticket.getTicketId(), ticket.getFromCity(), ticket.getToCity(),
				ticket.getSeatNum());
	}

	@Override
	public int updateTicket(Ticket ticket) {

		return jdbcTemplate.update(QueryBuilder.UPDATE_SEAT_NUM_BY_ID, ticket.getSeatNum(), ticket.getTicketId());
	}

	@Override
	public List<Ticket> selectAll() {
		RowMapper<Ticket> rowMapper = new TicketRowMapper();
		return jdbcTemplate.query(QueryBuilder.SELECT_ALL, rowMapper);
	}

	@Override
	public Ticket selectTicketById(int id) {
		RowMapper<Ticket> rowMapper = new TicketRowMapper();
		return jdbcTemplate.queryForObject(QueryBuilder.SELECT_BY_ID, new Object[] { id }, rowMapper);
	}

	@Override
	public int deleteTicketById(int id) {
		return jdbcTemplate.update(QueryBuilder.DELETE_BY_ID, id);
	}

	@Override
	public int deleteAllTickets() {
		// TODO Auto-generated method stub
		return jdbcTemplate.update(QueryBuilder.DELETE_ALL);
	}

}