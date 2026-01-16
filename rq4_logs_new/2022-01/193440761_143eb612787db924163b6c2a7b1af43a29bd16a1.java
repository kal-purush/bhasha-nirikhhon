package demo.service.impl;

import demo.model.Entity;
import demo.repo.EntityRepository;
import demo.repo.UserRepository;
import demo.service.EntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.logging.Logger;

@Service
public class EntityServiceImpl implements EntityService {

    private final static Logger LOGGER = Logger.getLogger(EntityServiceImpl.class.getName());

    @Autowired
    private EntityRepository entityRepository;

    @Autowired
    private UserRepository userRepository;

    @Override
    public void saveEntities(List<Entity> jobs) {
        List<Entity> jobList = entityRepository.saveAll(jobs);
    }

    @Override
    public Entity saveEntity(Entity job) {
        return entityRepository.save(job);
    }

    @Override
    public void deleteEntity(Entity job) {
        entityRepository.delete(job);
    }

    @Override
    public List<Entity> getJobsByUserId(String userId) {
        return null;
    }

    @Override
    public List<Entity> getAllActiveEntities() {
       return entityRepository.findAll();
    }
}