package com.linepro.modellbahn.converter.request;

import static com.linepro.modellbahn.ModellBahnApplication.PREFIX;

import org.springframework.stereotype.Component;

import com.linepro.modellbahn.converter.impl.MapperImpl;
import com.linepro.modellbahn.converter.request.transcriber.NamedRequestTranscriber;
import com.linepro.modellbahn.entity.Achsfolg;
import com.linepro.modellbahn.request.AchsfolgRequest;

@Component(PREFIX + "AchsfolgRequestMapper")
public class AchsfolgRequestMapper extends MapperImpl<AchsfolgRequest, Achsfolg> {

    public AchsfolgRequestMapper() {
        super(() -> new Achsfolg(), new NamedRequestTranscriber<>());
    }
}