package com.example.lifeonhana.dto.response;

import com.example.lifeonhana.entity.History.Category;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public record HistoryDetailResponseDTO(
	@JsonProperty("historyId") Long historyId,
	@JsonProperty("category") Category category,
	@JsonProperty("amount") BigDecimal amount,
	@JsonProperty("description") String description,
	@JsonProperty("historyDateTime") LocalDateTime historyDateTime,
	@JsonProperty("isFixed") Boolean isFixed,
	@JsonProperty("isExpense") Boolean isExpense
) {}