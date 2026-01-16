package sanrocks.tradingbot.config;

import java.util.List;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.support.composite.UnitRuleGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import sanrocks.tradingbot.domain.rules.ProductRuleEngine;
import sanrocks.tradingbot.rules.product.ProductBaseRules;

@Configuration
public class RuleEngineConfig {

    /**
     * @param productBaseRules
     * @return
     */
    @Bean("productRuleEngine")
    public ProductRuleEngine productRules(@Autowired List<ProductBaseRules> productBaseRules) {

        Rules rules = new Rules();

        UnitRuleGroup unitRuleGroup = new UnitRuleGroup("product-sell-flow");
        productBaseRules.forEach(unitRuleGroup::addRule);

        //rules.register(unitRuleGroup);
        productBaseRules.forEach(rules::register);

        return new ProductRuleEngine(rules, new DefaultRulesEngine());
    }
}