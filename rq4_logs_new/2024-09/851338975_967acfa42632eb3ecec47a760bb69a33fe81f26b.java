package com.reservibe.infra.adapter.table;

import com.reservibe.domain.enums.table.TableStatus;
import com.reservibe.infra.model.table.TableModel;
import com.reservibe.infra.repository.table.TableModelRepository;
import jakarta.persistence.EntityNotFoundException;

import java.util.UUID;

public class SearchTableByIdAdapter {
    private final TableModelRepository tableModelRepository;

    public SearchTableByIdAdapter(TableModelRepository tableModelRepository) {
        this.tableModelRepository = tableModelRepository;
    }

    public TableModel getTableByIdAndStatusIsFree(UUID id) {
        return tableModelRepository.findByIdAndStatus(id, TableStatus.FREE)
                .orElseThrow(()->new EntityNotFoundException("Table not Available"));
    }
}
