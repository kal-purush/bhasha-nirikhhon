
from estm_model_checker.estm_rule_parser import ESTMRuleParser
from estm_model_checker.model_checker import models_for_map, format_models
from itertools import combinations

def main():
    img = [
            'LLLLLLLLLL',
            'LLLLLLLLLL',
            'LLLLLLLLLL',
            'LLLLLLLLLL',
            'LLLLLCLLCC',
            'LLLLCSCCSS',
            'LLLCSSSSSS',
            'LLCSSSSSSS',
            'LCSSSSSSSS',
            'CSSSSSSSSS'
            ]



    rule_parser = ESTMRuleParser(img)
    rules,_,literals = rule_parser.calc_rules()
    map_size = (2,1)
    _, models = models_for_map(map_size=map_size, literals=literals, rule_dict=rules)
    model_list = list(models)
    print('*** MODELS ***')
    print(model_list)
    print('*** MODELS FOR RULESET ***')
    m, sorted_keys = format_models(model_list)
    print(",".join(sorted_keys))
    for mod in m:
        print(mod)

if __name__ == "__main__":
    main()