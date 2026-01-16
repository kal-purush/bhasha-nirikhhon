package com.gitlab.tomaszgryczka.fungiseeker.application.dtos;

import com.gitlab.tomaszgryczka.fungiseeker.domain.mushroom.MushroomPrediction;
import lombok.Builder;
import lombok.NonNull;

@Builder
public record PredictorDTO(@NonNull Long id,
                           @NonNull String name,
                           @NonNull Double probability) {

    public MushroomPrediction.MushroomPredictionBuilder toMushroomPredictionBuilder() {
        return MushroomPrediction.builder()
                .mushroomPredictionId(id)
                .name(name)
                .probability(probability);
    }
}