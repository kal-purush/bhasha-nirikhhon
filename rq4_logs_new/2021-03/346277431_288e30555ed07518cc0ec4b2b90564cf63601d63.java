package hr.tvz.keepthechange.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Jxls related configuration.
 */
@Configuration
public class JxlsConfig {

    /**
     * Registers a {@link SpelExpressionParser} for parsing Spring Expression Language expressions.
     */
    @Bean
    public SpelExpressionParser spelExpressionParser() {
        return new SpelExpressionParser();
    }

    /**
     * Registers a {@link BeanFactoryResolver} which makes it possible to access spring configured beans inside SpEL.
     *
     * @param applicationContext contains configured beans
     */
    @Bean
    public BeanFactoryResolver beanFactoryResolver(ApplicationContext applicationContext) {
        return new BeanFactoryResolver(applicationContext);
    }

    /**
     * Configures a {@link StandardEvaluationContext} used in evaluation of SpEL expressions.
     * <p>
     * New instance of this bean should be created for every evaluation that has specific variables.
     *
     * @param beanFactoryResolver used to access beans from context
     */
    @Bean
    @Scope("prototype")
    public StandardEvaluationContext standardEvaluationContext(BeanFactoryResolver beanFactoryResolver) {
        StandardEvaluationContext c = new StandardEvaluationContext();
        c.setBeanResolver(beanFactoryResolver);
        return c;
    }

    /**
     * Configures a {@link SpelExpressionEvaluator} which evaluates SpEL expression.
     *
     * @param spelExpressionParser      parses SpEL expressions passed to {@link SpelExpressionEvaluator}
     * @param standardEvaluationContext context for evaluation of expressions passed to {@link SpelExpressionEvaluator}
     */
    @Bean
    @Scope("prototype")
    public SpelExpressionEvaluator spelExpressionEvaluator(SpelExpressionParser spelExpressionParser,
                                                           StandardEvaluationContext standardEvaluationContext) {
        return new SpelExpressionEvaluator(spelExpressionParser, standardEvaluationContext);
    }
}