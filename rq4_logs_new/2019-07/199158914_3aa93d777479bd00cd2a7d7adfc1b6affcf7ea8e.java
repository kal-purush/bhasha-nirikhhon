package br.com.newbietrader.adapter;

import br.com.newbietrader.dto.StockDTO;
import br.com.newbietrader.dto.StockValueDTO;
import br.com.newbietrader.entity.Stock;
import br.com.newbietrader.entity.StockValue;

public class StockAdapter {

	public static Stock toEntity(StockDTO dto) {
		
		Stock stock = new Stock();
		stock.setDate(dto.getDate());
		stock.setName(dto.getName());
		StockValueDTO valueDto = dto.getValue();
		stock.setValue(StockValue.of(valueDto.getStart(), valueDto.getEnd()));
		
		return stock;
	}
	
	public static StockDTO toDTO(Stock stock) {
		
		StockDTO dto = new StockDTO();
		dto.setDate(stock.getDate());
		dto.setName(stock.getName());
		StockValue value = stock.getValue();
		
		StockValueDTO valueDTO = new StockValueDTO();
		
		valueDTO.setStart(value.getStart());
		valueDTO.setEnd(value.getEnd());
		dto.setValue(valueDTO);
		
		return dto;
	}
	
	
}