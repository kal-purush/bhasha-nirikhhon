/* Licensed under Apache-2.0 2024. */
package github.benslabbert.vertxdaggeriam.config;

import dagger.Module;
import dagger.Provides;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import javax.inject.Singleton;
import org.hibernate.validator.HibernateValidator;

@Module
final class HibernateValidationConfig {

  private HibernateValidationConfig() {}

  @Singleton
  @Provides
  static ValidatorFactory validatorFactory() {
    return Validation.byProvider(HibernateValidator.class)
        .configure()
        .failFast(false)
        .buildValidatorFactory();
  }
}