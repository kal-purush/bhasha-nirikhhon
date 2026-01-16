package com.marketplace.domain.classifiedad.query;

import com.marketplace.domain.classifiedad.ClassifiedAd;
import com.marketplace.domain.classifiedad.entity.ClassifiedAdEntity;
import com.marketplace.domain.repository.MongoTemplate;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static com.mongodb.client.model.Filters.eq;

@Named
@Singleton
public class ClassifiedAdMongoQueryRepositoryImpl implements ClassifiedAdMongoQueryRepository {

  private final MongoTemplate mongoTemplate;
  private final String collectionName = ClassifiedAd.class.getSimpleName().toLowerCase();

  @Inject
  public ClassifiedAdMongoQueryRepositoryImpl(MongoTemplate mongoTemplate) {
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  public Optional<ClassifiedAdQueryEntity> findById(UUID id) {
    Optional<ClassifiedAdEntity> found =
        mongoTemplate.findById(id, collectionName, ClassifiedAdEntity.class);
    return found.map(ClassifiedAdEntity::toClassifiedAdReadEntity);
  }

  @Override
  public List<ClassifiedAdQueryEntity> findAll() {
    List<ClassifiedAdEntity> all = mongoTemplate.findAll(collectionName, ClassifiedAdEntity.class);
    return convert(all);
  }

  @Override
  public List<ClassifiedAdQueryEntity> findByOwner(UUID ownerId) {
    List<ClassifiedAdEntity> all =
        mongoTemplate.findByQuery(collectionName, eq("owner", ownerId), ClassifiedAdEntity.class);
    return convert(all);
  }

  @Override
  public List<ClassifiedAdQueryEntity> find(Bson filter) {
    var all = mongoTemplate.findByQuery(collectionName, filter, ClassifiedAdEntity.class);
    return convert(all);
  }

  List<ClassifiedAdQueryEntity> convert(List<ClassifiedAdEntity> entities) {
    return entities.stream()
        .map(ClassifiedAdEntity::toClassifiedAdReadEntity)
        .collect(Collectors.toList());
  }
}