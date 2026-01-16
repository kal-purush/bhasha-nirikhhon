package ru.clean.process.db.actors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.clean.process.api.dto.actor.Actor;
import ru.clean.process.api.service.repo_adapters.ActorRepositoryAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter to work with user storage
 */
@Service
public class ActorRepositoryAdapterImpl implements ActorRepositoryAdapter {

    private ActorRepository actorRepository;

    @Autowired
    public ActorRepositoryAdapterImpl(ActorRepository actorRepository) {
        this.actorRepository = actorRepository;
    }

    @Override
    public List<Actor> getAllActors() {
        return new ArrayList<>(actorRepository.findAll());
    }
}