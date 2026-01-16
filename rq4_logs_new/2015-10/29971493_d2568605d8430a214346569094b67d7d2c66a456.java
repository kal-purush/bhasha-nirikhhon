/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.biz)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.authnr.qdsl.util;

import com.mysema.query.types.Expression;
import com.mysema.query.types.expr.BooleanExpression;

/**
 * Utility API to create the permission checks easily for Querydsl based SQL queries.
 */
public interface AuthnrQdslUtil {

  /**
   * Creates a predicate for a Querydsl based SQL query. If the predicate is appended to the where
   * part of the main query, the results will be filtered based on authorization. The
   * authorizedResourceId must be provided by the implementation, that is typically the same as the
   * authenticated resourceId.<br>
   * <br>
   * E.g.: Users and books are resources. Users can view and edit books. To list the books that the
   * currently authenticated user can view or edit, the following code snippet should be used:
   *
   * <pre>
   * QBook book = QBook.book;
   * BooleanExpression authrPredicate =
   *         authnrQdslUtil.authorizationPredicate(book.resourceId, "read", "edit");
   * 
   * SQLQuery query = new SQLQuery(connection, configuration);
   * 
   * return query.from(book)...where(authrPredicate)...list(...);
   * </pre>
   *
   * @param targetResourceId
   *          Expression that provides the resource that the permission is defined on. This
   * @param actions
   *          If the authorized resource has permission to do any of the actions on the target
   *          resource record (directly or by inheritance), the result will provide
   *          <code>true</code>.
   * @return An expression that can be used as a predicate in the main query.
   * @throws NullPointerException
   *           if the targetResourceId parameter is null, the actions parameter is null or the
   *           actions array contains null value.
   * @throws IllegalArgumentException
   *           if the actions parameter is a zero length array.
   */
  BooleanExpression authorizationPredicate(Expression<Long> targetResourceId, String... actions);

}