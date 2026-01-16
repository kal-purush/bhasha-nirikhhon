package com.tomdaly.hotel.business.service;

import com.tomdaly.hotel.aspect.Loggable;
import com.tomdaly.hotel.data.entity.Profanity;
import com.tomdaly.hotel.data.repository.ProfanityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Service
public class ProfanityService {
  private final ProfanityRepository profanityRepository;
  private Set<Profanity> profanitySet;

  @Autowired
  public ProfanityService(ProfanityRepository profanityRepository) {
    this.profanityRepository = profanityRepository;
  }

  @Loggable
  public Profanity addProfanity(String word) {
    Profanity profanity = this.profanityRepository.findByWord(word);
    if (profanity == null) {
      profanity = new Profanity(word);
      this.profanityRepository.save(profanity);
    }
    return profanity;
  }

  @Loggable
  public boolean containsProfanity(String stringToCheck) {
    profanitySet = fillProfanitySetFromRepository(profanityRepository);
    for (Profanity profanity : profanitySet) {
      if (stringToCheck.equals(profanity.getWord())) {
        return true;
      }
    }
    return false;
  }

  private static Set<Profanity> fillProfanitySetFromRepository(ProfanityRepository repository) {
    Iterable<Profanity> iter = repository.findAll();
    return new HashSet<Profanity>((Collection) iter);
  }
}