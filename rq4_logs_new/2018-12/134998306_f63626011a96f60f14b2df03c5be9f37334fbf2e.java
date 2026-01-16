package xxx.joker.libs.core.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xxx.joker.libs.core.exception.JkRuntimeException;
import xxx.joker.libs.core.repository.entity.JkEntity;
import xxx.joker.libs.core.utils.JkReflection;
import xxx.joker.libs.core.utils.JkStuff;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static xxx.joker.libs.core.repository.JkPersistenceManager.EntityLines;

public interface IJkDataModel {

    void commit();

    void cascadeDependencies();
    void cascadeDependencies(Class<?> clazz);
    void cascadeDependencies(JkEntity entity);

}