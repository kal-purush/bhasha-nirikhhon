package ru.gworkshop.slhub.common.model.responce;

import ru.gworkshop.slhub.common.model.entity.Crate;

import java.util.List;

@SuppressWarnings("FieldCanBeLocal")
class CrateResponse
{
    private final Integer status;
    private final String state;
    private List<Crate> crates;

    public CrateResponse( Integer status, String state) {
        this.status = status;
        this.state = state;
    }

    public CrateResponse( Integer status, String state, List<Crate> crates) {
        this.status = status;
        this.state = state;
        this.crates = crates;
    }
}