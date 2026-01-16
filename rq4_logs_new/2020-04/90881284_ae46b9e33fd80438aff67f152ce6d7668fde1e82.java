package org.springframework.data.aerospike;

import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.stream.Stream;

public class QueryUtils {

	public static <T> Query createQueryForMethodWithArgs(String methodName, Object... args) {
		Class[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
		Method method = ReflectionUtils.findMethod(PersonRepository.class, methodName, argTypes);
		PartTree partTree = new PartTree(method.getName(), Person.class);
		AerospikeQueryCreator creator =
				new AerospikeQueryCreator(partTree,
						new ParametersParameterAccessor(
								new QueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
										new SpelAwareProxyProjectionFactory()).getParameters(), args));
		return creator.createQuery();
	}
}