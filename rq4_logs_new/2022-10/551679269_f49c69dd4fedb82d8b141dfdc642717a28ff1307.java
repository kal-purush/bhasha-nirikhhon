package com.mifag.app.dto;

import javax.validation.constraints.NotNull;

public class CreateOwnerKeyboardMapDto {
    @NotNull
    private Long ownerId;

    @NotNull
    private Long midiKeyboardId;

    public Long getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(Long ownerId) {
        this.ownerId = ownerId;
    }

    public Long getMidiKeyboardId() {
        return midiKeyboardId;
    }

    public void setMidiKeyboardId(Long midiKeyboardId) {
        this.midiKeyboardId = midiKeyboardId;
    }
}