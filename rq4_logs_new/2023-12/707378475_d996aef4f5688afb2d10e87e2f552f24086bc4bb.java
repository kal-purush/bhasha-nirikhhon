package com.gitlab.tomaszgryczka.fungiseeker.application.dtos;

import com.gitlab.tomaszgryczka.fungiseeker.domain.labels.MushroomLabels;
import lombok.NonNull;

public record MushroomLabelDTO (@NonNull Long id,
                                @NonNull String name,
                                @NonNull Boolean isEdible) {

    public static MushroomLabelDTO fromMushroomLabel(Long id, MushroomLabels.MushroomLabel mushroomLabel) {
        return new MushroomLabelDTO(
                id,
                mushroomLabel.getTranslation(),
                mushroomLabel.getEdibility() == MushroomLabels.Edibility.EDIBLE
        );
    }
}