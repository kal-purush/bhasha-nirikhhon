import re
import shared.regexUtils as regexUtils
from book_extractors.common.extraction_keys import KEYS
from book_extractors.common.extractors.base_extractor import BaseExtractor


class BoolExtractor(BaseExtractor):
    extraction_key = KEYS['flags']

    def __init__(self, key_of_cursor_location_dependent, options):
        super(BoolExtractor, self).__init__(key_of_cursor_location_dependent, options)
        self.patterns_to_find = options['patterns']
        self.OPTIONS = (re.UNICODE | re.IGNORECASE)

    def _extract(self, entry, extraction_results, extraction_metadata):
        result = self._find_patterns(entry['text'])
        return self._add_to_extraction_results(result, extraction_results, extraction_metadata, 0)

    def _find_patterns(self, text):
        results = {}
        for key, pattern in self.patterns_to_find.items():
            try:
                regexUtils.safe_search(pattern, text, self.OPTIONS)
                results[key] = True
            except regexUtils.RegexNoneMatchException:
                results[key] = False
                pass
        return results