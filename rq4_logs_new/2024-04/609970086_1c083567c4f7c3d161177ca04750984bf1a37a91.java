package com.bearAndPupperCo.sangenWrestlingApp.Enum;

import com.bearAndPupperCo.sangenWrestlingApp.Exception.WrongParamException;

import java.util.Arrays;

import static com.bearAndPupperCo.sangenWrestlingApp.APIUtils.MessageConstants.WRONG_PARAM_MSG;

public enum WrestlerMainTableEnum {

    NAME("in_ring_name"),
    VICTORIES("total_victories"),
    LOSSES("total_losses"),
    STREAK("wrestling_streak"),
    WIN_PERCENTAGE("percentage_of_victories");

    private final String columnName;

    WrestlerMainTableEnum(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public static String getColumnName(String enumValue) {
        return Arrays.stream(WrestlerMainTableEnum.values())
                .filter(columnEnum -> columnEnum.name().equalsIgnoreCase(enumValue))
                .findFirst()
                .map(WrestlerMainTableEnum::getColumnName)
                .orElseThrow(() -> new WrongParamException(WRONG_PARAM_MSG));
    }

}