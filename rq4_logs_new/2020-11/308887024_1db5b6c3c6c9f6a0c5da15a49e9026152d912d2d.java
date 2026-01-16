package com.marketplace.command;

import com.marketplace.controller.UpdateAdDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateClassifiedAd implements Command {
    private UUID id;

    public static UpdateClassifiedAd from(UpdateAdDto updateAdDto) {
        return null;
    }

    @Override
    public Instant createdAt() {
        return null;
    }
}