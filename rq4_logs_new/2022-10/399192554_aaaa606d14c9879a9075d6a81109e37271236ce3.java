package com.github.ovorobeva.vocabularywordsservice.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ExcludedPartsOfSpeech {
    INTERJECTION ("interjection"),
    PRONOUN ("pronoun"),
    PREPOSITION ("preposition"),
    ABBREVIATION ("abbreviation"),
    AFFIX ("affix"),
    ARTICLE ("article"),
    AUXILIARY_VERB ("auxiliary-verb"),
    CONJUNCTION ("conjunction"),
    DEFINITE_ARTICLE ("definite-article"),
    FAMILY_NAME ("family-name"),
    GIVEN_NAME ("given-name"),
    IMPERATIVE ("imperative"),
    PROPER_NOUN ("proper-noun"),
    PROPER_NOUN_PLURAL ("proper-noun-plural"),
    SUFFIX ("suffix"),
    VERB_INTRANSITIVE ("verb-intransitive"),
    VERB_TRANSITIVE ("verb-transitive");

    private final String value;

}