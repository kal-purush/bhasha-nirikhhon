package io.github.mufasa1976.spring.angular2.example.assembler;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;

import org.springframework.hateoas.mvc.IdentifiableResourceAssemblerSupport;

import io.github.mufasa1976.spring.angular2.example.controller.HeroController;
import io.github.mufasa1976.spring.angular2.example.model.HeroEntity;
import io.github.mufasa1976.spring.angular2.example.resource.HeroResource;
import org.springframework.stereotype.Component;

@Component
public class HeroResourceAssembler extends IdentifiableResourceAssemblerSupport<HeroEntity, HeroResource> {

  public HeroResourceAssembler() {
    super(HeroController.class, HeroResource.class);
  }

  @Override
  public HeroResource toResource(HeroEntity entity) {
    HeroResource resource = HeroResource.builder()
        .version(entity.getVersion())
        .createdBy(entity.getCreatedBy())
        .createdAt(entity.getCreatedAt())
        .lastModifiedBy(entity.getLastModifiedBy())
        .lastModifiedAt(entity.getLastModifiedAt())
        .id(entity.getId())
        .name(entity.getName())
        .build();
    resource.add(linkTo(HeroController.class, entity.getId()).slash(entity.getId()).withSelfRel());
    return resource;
  }
}