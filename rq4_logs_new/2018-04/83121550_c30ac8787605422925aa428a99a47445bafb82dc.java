package no.fint.relations.hateoas;

import no.fint.model.resource.FintLinks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class FintHateoasLinkAssembler {

    @Autowired
    private List<HateoasLinks> hateoasLinks;
    private Map<Class<?>, HateoasLinks> hateoasLinksMap = new HashMap<>();

    @PostConstruct
    public void init() {
        hateoasLinks.forEach(h -> hateoasLinksMap.put(h.getModel(), h));
    }

    public FintLinks assemble(FintLinks fintLinks) {
        HateoasLinks hateoasLinks = hateoasLinksMap.get(fintLinks.getClass());
        return hateoasLinks.assemble(fintLinks);
    }

}