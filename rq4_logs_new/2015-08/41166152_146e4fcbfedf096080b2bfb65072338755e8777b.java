package eu.horako.stemmer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class to find words derived from a stem. Because the expansion rules may be 
 * recursive, it's required to explicitly set the depth for the rules application.
 * 
 * Basic usage: 
 * Create the AffixExpander with a Dictionary and AffixRuleSet and 
 * call {@see expand(String,int)} on each word you want to expand.
 * 
 * 
 * @author Ondrej Horak &lt;ondrej.horak@centrum.cz&gt;
 */
public class AffixExpander implements IAffixProcessor {
    private final AffixRuleSet ruleSet;
    private final Dictionary dictionary;
    private int processingDepth = 1;
    private boolean stickyExpanded = false;
    private boolean noloop = true;

    
    /**
     * Create the expander from rule set and dictionary
     * @param ruleSet
     * @param dictionary 
     */
    public AffixExpander(AffixRuleSet ruleSet, Dictionary dictionary) {
        this.ruleSet = ruleSet;
        this.dictionary = dictionary;
        //this.emptyRule = new AffixRule(AffixRuleType.SFX,"","","",null,".",true,null);
    }
    
    /**
     * Use this method to find all derivations of the stem up to given depth;
     * as for now, the depth is the limit of the sum of prefix and suffix 
     * expansions.
     * 
     * @param word word to expand
     * @param depth depth of expansion (total limit for the prefix and suffix)
     * @return set of expanded words
     */
    public Set<String> expand(String word,int depth){
        if(!stickyExpanded) expandStickyRules();
        
        Set<String> validWords = new HashSet<String>();
        Set<String> invalidWords = new HashSet<String>();
        if(!this.dictionary.contains(word)) { return validWords; }
        validWords.add(word);
        List<Set<String>> allFlags = this.dictionary.getAllFlags(word);
        List<ExpansionRules> startingExpansions = new ArrayList<ExpansionRules>();
        
        for(Set<String> flags : allFlags) {
            Set<AffixRule> rules = new HashSet<AffixRule>();
            for(String flag: flags) {
                Set<AffixRule> rulesByFlag = this.ruleSet.rulesByFlag.get(flag);
                if(rulesByFlag != null) {
                    rules.addAll(this.ruleSet.rulesByFlag.get(flag));
                }
            }
            startingExpansions.add(new ExpansionRules(word,rules,false,false));
        }

        List<ExpansionRules> currentExpansions = startingExpansions;
        for(int recursionStep=0; recursionStep < depth; recursionStep++) {
            List<ExpansionRules> followingExpansions = new ArrayList<ExpansionRules>();
            for(ExpansionRules fe : currentExpansions) {
                followingExpansions.addAll(this.expandOneStep(fe.word, fe.expRules, fe.invalidSfx, fe.invalidPfx, validWords, invalidWords));
            }
            currentExpansions = followingExpansions;
        }
        return validWords;
    }
    
    private void addWord(String word, boolean isInvalid, Set<String> valid, Set<String> invalid) {
        if(isInvalid) {
            if(!valid.contains(word)) { invalid.add(word); }
        } else {
            invalid.remove(word);
            valid.add(word);
        }
    }
    
    // input: word + expRules set
    // input/output - valid + invalid words
    private List<ExpansionRules> expandOneStep(String word, Collection<AffixRule> rules, 
            boolean invalidSfx, boolean invalidPfx,
            Set<String> validWords, Set<String> invalidWords) {
        String newSfxWord, newPfxWord, newCrossWord;

        List<ExpansionRules> followExpansions = new ArrayList<ExpansionRules>();
        for(AffixRule sfxRule : rules) {
            if(sfxRule.getType() != AffixRuleType.SFX) continue;
            if(sfxRule.isSticky()) continue;
            
            newSfxWord = sfxRule.apply(word);
            if(newSfxWord == null) { continue; }

            addWord(newSfxWord,sfxRule.isInvalid() || invalidPfx,validWords,invalidWords); // process word - add to valid / invalid

            AffixMap<String,AffixRule> followSfxRules = sfxRule.getExpansionRules();
            if(followSfxRules!=null && followSfxRules.size()>0) {
                followExpansions.add(new ExpansionRules(newSfxWord,followSfxRules.getAll(),sfxRule.isInvalid(),invalidPfx));
            }

            if(sfxRule.isCrossProduct()) {
                for(AffixRule pfxRule : rules) {
                    if(pfxRule.getType() != AffixRuleType.PFX || !pfxRule.isCrossProduct()) continue;
                    if(pfxRule.isSticky()) continue;

                    newCrossWord = pfxRule.apply(newSfxWord);
                    if(newCrossWord == null) { continue; }

                    addWord(newCrossWord,pfxRule.isInvalid() || sfxRule.isInvalid(),validWords,invalidWords); // process word - add to valid / invalid

                    ExpansionRules expRules = null;
                    if(followSfxRules!=null && followSfxRules.size()>0) {
                       expRules = new ExpansionRules(newCrossWord,followSfxRules.getAll(),sfxRule.isInvalid(),pfxRule.isInvalid());
                    }
                    AffixMap<String,AffixRule> followPfxRules = pfxRule.getExpansionRules();
                    if(followPfxRules!=null && followPfxRules.size()>0) {
                        if(expRules != null) { 
                            expRules.addRules(followPfxRules.getAll());                            
                        }
                        else {
                            expRules =  new ExpansionRules(newCrossWord,followPfxRules.getAll(),sfxRule.isInvalid(),pfxRule.isInvalid());
                        }
                    }
                    if(expRules != null) { followExpansions.add(expRules); }
               }
            }
        }

        for(AffixRule pfxRule : rules) {
            if(pfxRule.getType() != AffixRuleType.PFX) continue;
            if(pfxRule.isSticky()) continue;

            newPfxWord = pfxRule.apply(word);
            if(newPfxWord == null) { continue; }
            addWord(newPfxWord,pfxRule.isInvalid() || invalidSfx,validWords,invalidWords); // process word - add to valid / invalid
            AffixMap<String,AffixRule> followPfxRules = pfxRule.getExpansionRules();
            if(followPfxRules!=null && followPfxRules.size()>0) {
                   followExpansions.add(new ExpansionRules(newPfxWord,followPfxRules.getAll(),invalidSfx,pfxRule.isInvalid()));
            }
        }
        return followExpansions;
    }


    
    @Override
    public Set<String> process(String word) {
        return this.expand(word, this.processingDepth);
    }
    
    public void setProcessingDepth(int processingDepth) {
        this.processingDepth = processingDepth;
    }

    public void setNoloop(boolean noloop) {
        this.noloop = noloop;
    }
    
    // one-level sticky expansion
    public void expandSticky1(String word, Set<String> flags, Dictionary outputDict) {
        for(String s : flags) {
            Set<AffixRule> rules = this.ruleSet.getRulesByFlag(s);
            for(AffixRule rule : rules) {
                if(rule.isSticky()) {
                    String newWord = rule.apply(word);
                    if(newWord == null) continue;
                    Set<String> otherFlags = new HashSet<String>();
                    for(String otherFlag : flags) {
                        if(ruleSet.getRuleTypeByFlag(otherFlag) != rule.getType()) otherFlags.add(otherFlag);
                    }
                    
                    Set<String> expFlags = rule.getExpansionFlags();
                    if(expFlags != null) {
                        for(String expFlag : expFlags) {
                           if(!expFlag.equals(rule.getFlag())) otherFlags.add(expFlag);
                        }
                    }
                    outputDict.add(newWord, otherFlags);
                }
            }
        }
    }

    
    public void expandStickyRules() {
        Dictionary newDict = new Dictionary();
        for(String w : this.dictionary.getWords()) {
            for(Set<String> flags : this.dictionary.getAllFlags(w)) {
                expandSticky1(w, flags,newDict);
            }
        }
        
        for(String w : newDict.getWords()) {
            for(Set<String> flags : newDict.getAllFlags(w)) {
                dictionary.add(w, flags);
            }
        }
        stickyExpanded = true;
    }

    /**
     * Encapsulates word and expRules that can be applied to this word
     * Represents one step in the expansion (with expRules leading to next steps)
     * 
     */
    class ExpansionRules {
        String word;
        Set<AffixRule> expRules;
        boolean invalidPfx = false;
        boolean invalidSfx = false;
        
        public ExpansionRules(String word, Set<AffixRule> expansionRules, boolean invalidSfx, boolean invalidPfx) {
            this.word = word;
            this.expRules = new HashSet<AffixRule>(expansionRules);
            this.invalidSfx = invalidSfx;
            this.invalidPfx = invalidPfx;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("    RULES FOR '").append(this.word).append("'");
            sb.append(" SFX " ).append(this.invalidSfx?"in":"").append("valid");
            sb.append(" PFX " ).append(this.invalidPfx?"in":"").append("valid");
            sb.append(": ");
            for(AffixRule r: this.expRules) {
                sb.append(r.getFlag()).append(" ");
            }
            return sb.toString();
        }
        
        public void addRules(Set<AffixRule> rules) {
            this.expRules.addAll(rules);
        }
    }

}