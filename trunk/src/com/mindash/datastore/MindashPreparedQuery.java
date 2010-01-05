/**
 * Copyright 2010 Tristan Slominski
 * 
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.opensource.org/licenses/mit-license.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mindash.datastore;

import java.util.Iterator;
import java.util.List;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.PreparedQuery.TooManyResultsException;

/**
 * <p>
 * The wrapper interface for Google's
 * {@link com.google.appengine.api.datastore.PreparedQuery}.
 * </p>
 * <p>
 * Because in order to retrieve the queries an assembly of entities from shards
 * is required, there is no computational benefit of choosing {@code
 * asIterable()} over {@code asList()}.
 * </p>
 * 
 * @author Tristan Slominski
 */
public interface MindashPreparedQuery {

  public Iterable<Entity> asIterable();

  public Iterable<Entity> asIterable(FetchOptions fetchOptions);

  public Iterator<Entity> asIterator();

  public Iterator<Entity> asIterator(FetchOptions fetchOptions);

  public List<Entity> asList(FetchOptions fetchOptions);

  public QueryResultIterable<Entity> asQueryResultIterable()
      throws NotImplementedException;

  public QueryResultIterable<Entity> asQueryResultIterable(
      FetchOptions fetchOptions) throws NotImplementedException;

  public QueryResultIterator<Entity> asQueryResultIterator()
      throws NotImplementedException;

  public QueryResultIterator<Entity> asQueryResultIterator(
      FetchOptions fetchOptions) throws NotImplementedException;

  public QueryResultList<Entity> asQueryResultList(FetchOptions fetchOptions)
      throws NotImplementedException;

  public Entity asSingleEntity() throws TooManyResultsException;

  public int countEntities();

}
