package no.fint.relations;

import no.fint.model.resource.FintLinks;
import no.fint.model.resource.Link;
import no.fint.relations.internal.FintLinkMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;

public abstract class FintLinker<T extends FintLinks> {
    private final Class<?> controllerClass;

    @Autowired
    private FintLinkMapper linkMapper;

    public FintLinker(Class<?> controllerClass) {
        this.controllerClass = controllerClass;
    }

    public FintResources toResources(List<T> resources) {
        List<FintResource> fintResources = resources.stream().map(this::toResource).collect(Collectors.toList());
        return new FintResources(fintResources, self());
    }

    public FintResource toResource(T resource) {
        mapLinks(resource);

        String self = getSelfHref(resource);
        resource.addLink(org.springframework.hateoas.Link.REL_SELF, Link.with(self));
        return new FintResource(resource);
    }

    private void mapLinks(FintLinks resource) {
        if (resource != null) {
            Map<String, List<Link>> links = resource.getLinksIfPresent();
            links.values().forEach(
                    linkValues -> linkValues.forEach(
                            link -> link.setVerdi(linkMapper.getLink(link.getHref()))
                    )
            );

            List<FintLinks> nestedResources = resource.getNestedResources();
            if (nestedResources.size() > 0) {
                nestedResources.forEach(this::mapLinks);
            }
        }
    }

    public String createLinkWithId(Object id, String path) {
        return linkTo(controllerClass).slash(path).slash(id).withSelfRel().getHref();
    }

    private String self() {
        org.springframework.hateoas.Link self = ControllerLinkBuilder.linkTo(controllerClass).withSelfRel();
        return linkMapper.populateProtocol(self).getHref();
    }

    public abstract String getSelfHref(T resource);

}