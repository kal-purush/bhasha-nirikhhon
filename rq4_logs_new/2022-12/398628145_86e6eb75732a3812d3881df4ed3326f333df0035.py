from src.tajweed_rules_helpers.punctuation_marks import PunctuationMarks

class MaddRules():
  """Madd Rules
  Madd means to lenghten the letters of madd or leen. 
  The letters of madd are "ا و ى".
  Alif saakin must be preceded by a letter carrying fatha: عَا
  Yaa saakin must be preceded by a letter carrying kasra: عِي
  Wow saakin must be preceded by a letter carrying dummah: عُو
  
  There are 2 broad types of madd:
  A. Madd Asli: 2 beats
    - "ا و ى" not preceded by hamza
    - "ا و ى" not followed by hamza or sukoon
  B. Madd Far'i
  

  Properties:
    *_surah_number: number of the surah (Quran chapter) where the verse being mapped is found
    *_ayah_number: number of the ayah (verse) being mapped
    *_ayah_text: text of the ayah (verse) being mapped

  Methods:
    *_find_final_index - finds the index of the last letter in an ayah, after excluding any marks that might follow it (vowels, meem of iqlab, stop signs)
    *_find_madd_letter_in_text - in a given ayah text, find all madd letters (ا و ى) instances and save the madd's indices to a list
    *_get_rule_location_details - get the details of a rule's location in the Quran (surah, ayah, beginning index, ending index)
    *_factory_for_finding_ending_letter_type - selects which method to run depending on which rule we are looking for
    *_is_ending_letter_an_idghaam_letter - checks if letter following meem saakin is "م" (meem)
    *_is_ending_letter_an_ikhfa_letter - checks if letter following meem saakin is "ب" (baa)
    *_is_ending_letter_an_idhaar_letter - checks if letter following meem saakin is anything but "م" or "ب"

    *get_all_rule_locations (public) - for a given rule, get the details of all its locations and save to a list
  """

  def __init__(self):
    self
    self._surah_number = None
    self._ayah_number = None
    self._ayah_text = None
    self.rules = ['madd_asli']
    self.punctuation_marks = PunctuationMarks()
    
  @property
  def surah_number(self):
    return self._surah_number
  @surah_number.setter
  def surah_number(self, value):
    self._surah_number = value
    
  @property
  def ayah_number(self):
    return self._ayah_number
  @ayah_number.setter
  def ayah_number(self, value):
    self._ayah_number = value
    
  @property
  def ayah_text(self):
    return self._ayah_text
  @ayah_text.setter
  def ayah_text(self, value):
    self._ayah_text = value
    
  def get_all_rule_locations(self, rule):
    """For a given rule, get the details of all its location and save to a list
      - parameters: rule (madd_asli)
      - returns: list of dicts:
      [{
        'surah': surah number,
        'ayah': ayah number,
        'start': starting letter index (the meem saakin itself)
        'end': ending letter index + 2 (the 2 is to also cover that letter's vowel)
      }]
    """
    if len(self._find_madd_letter_in_text()) == 0:
      return []

    all_rule_locations = []

    for index in self._find_madd_letter_in_text():
      rule_location = self._get_rule_location_details(index, rule)
      if rule_location:
        all_rule_locations.append(rule_location)

    return all_rule_locations

  def _find_madd_letter_in_text(self):
    """In an ayah (verse), find all madd letters (ا و ى) instances and save the madd letter's indices to a list.
    A madd letter is bare.
    For a given madd letter:
      *letter_index: the madd letter's index
      *self.ayah_text[letter_index + 1] not in 
      (self.punctuation_marks.non_sukoon_vowels + self.punctuation_marks.sukoon)): the madd letter is bare
      *letter in "ٰۦۥ": accounts for dagger and small variations of the madd letters
      - retuns: list of indices [3, 5, 8]
    """
    indices_for_madd = [
      letter_index for letter_index, letter in enumerate(self.ayah_text)
      if (letter in ["ا", "و", "ى", "ي"] and self.ayah_text[letter_index + 1] not in 
      (self.punctuation_marks.non_sukoon_vowels + self.punctuation_marks.sukoon)) or (letter in "ٰۦۥ")
    ]
    print(indices_for_madd)
    return indices_for_madd
  
  def _find_final_index(self):
    """Finds the index of the last letter in an ayah (verse), after excluding any diacritical marks that might follow it
      (vowels, meem of iqlab, stop signs).
      - retuns: index as integer
    """
    return self.punctuation_marks.calculate_adjustment_from_end(self.ayah_text)