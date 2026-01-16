/**
 * Copyright {2017} NativeChef
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.chefhub.repositories;

import io.chefhub.domain.Member;
import org.springframework.data.neo4j.repository.GraphRepository;

/**
 * <h1>{@link io.chefhub.repositories.MemberRepository}</h1>
 *
 * Repository interface that implements {@link GraphRepository}
 * of type Member.
 * <p>
 * Blueprint for standard query methods for the member domain including
 * relationsips with other domain nodes.
 * <p>
 *
 * @author  NativeChef
 * @version 0.0.1
 * @since   0.0.1
 */
public interface MemberRepository extends GraphRepository<Member> {
}