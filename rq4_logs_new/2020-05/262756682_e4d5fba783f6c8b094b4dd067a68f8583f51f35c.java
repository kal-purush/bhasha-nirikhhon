package com.ktech.di;

import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.ktech.repositories.PlanetRepository;

/**
 * Class that manages what type to instantiate at annotation @Inject
 * @author aleal
 *
 */
public class AppInjectionBinder extends AbstractBinder {

  @Override

  protected void configure() {

    bind(PlanetRepository.class).to(PlanetRepository.class);

  }

}