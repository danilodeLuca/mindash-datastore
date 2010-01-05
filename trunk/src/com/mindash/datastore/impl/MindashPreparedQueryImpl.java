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
package com.mindash.datastore.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.PreparedQuery.TooManyResultsException;
import com.google.inject.Inject;
import com.mindash.datastore.EntityCorruptException;
import com.mindash.datastore.MindashDatastoreService;
import com.mindash.datastore.MindashPreparedQuery;
import com.mindash.datastore.NotImplementedException;

/**
 * @author Tristan Slominski
 * 
 */
public class MindashPreparedQueryImpl implements MindashPreparedQuery {
  private DatastoreService datastore;
  private MindashDatastoreService mindashDatastore;
  private Query query;
  private Transaction txn;

  @Inject
  public MindashPreparedQueryImpl(DatastoreService datastore,
      MindashDatastoreService mindashDatastore, Query query, Transaction txn) {
    this.datastore = datastore;
    this.mindashDatastore = mindashDatastore;
    this.query = query;
    this.txn = txn;
  }

  @Override
  public Iterable<Entity> asIterable() {
    return retrieveSortedEntities(null);
  }

  @Override
  public Iterable<Entity> asIterable(FetchOptions fetchOptions) {
    return retrieveSortedEntities(fetchOptions);
  }

  @Override
  public Iterator<Entity> asIterator() {
    return retrieveSortedEntities(null).iterator();
  }

  @Override
  public Iterator<Entity> asIterator(FetchOptions fetchOptions) {
    return retrieveSortedEntities(fetchOptions).iterator();
  }

  @Override
  public List<Entity> asList(FetchOptions fetchOptions) {
    return retrieveSortedEntities(fetchOptions);
  }

  @Override
  public Entity asSingleEntity() throws TooManyResultsException {
    // we want only the key
    query.setKeysOnly();
    Entity result = null;
    if (txn != null) {
      result = datastore.prepare(txn, query).asSingleEntity();
    } else {
      result = datastore.prepare(query).asSingleEntity();
    }
    try {
      result = mindashDatastore.get(result.getParent());
    } catch (EntityNotFoundException e) {
      // TODO don't know how to deal with this yet
      e.printStackTrace();
    } catch (EntityCorruptException e) {
      // TODO don't know how to deal with this yet
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public int countEntities() {
    if (txn != null) {
      return datastore.prepare(txn, query).countEntities();
    } else {
      return datastore.prepare(query).countEntities();
    }
  }

  @Override
  public QueryResultIterable<Entity> asQueryResultIterable()
      throws NotImplementedException {
    // FIXME[Tristan]: implement
    throw new NotImplementedException();
  }

  @Override
  public QueryResultIterable<Entity> asQueryResultIterable(
      FetchOptions fetchOptions) throws NotImplementedException {
    // FIXME[Tristan]: implement
    throw new NotImplementedException();
  }

  @Override
  public QueryResultIterator<Entity> asQueryResultIterator()
      throws NotImplementedException {
    // FIXME[Tristan]: implement
    throw new NotImplementedException();
  }

  @Override
  public QueryResultIterator<Entity> asQueryResultIterator(
      FetchOptions fetchOptions) throws NotImplementedException {
    // FIXME[Tristan]: implement
    throw new NotImplementedException();
  }

  @Override
  public QueryResultList<Entity> asQueryResultList(FetchOptions fetchOptions)
      throws NotImplementedException {
    // FIXME[Tristan]: implement
    throw new NotImplementedException();
  }

  /**
   * @return
   */
  private List<Entity> retrieveSortedEntities(FetchOptions fetchOptions) {
    if (fetchOptions == null) {
      fetchOptions = FetchOptions.Builder.withLimit(1000);
    }
    // we want only keys
    query.setKeysOnly();
    List<Entity> results = null;
    if (txn != null) {
      results = datastore.prepare(txn, query).asList(fetchOptions);
    } else {
      results = datastore.prepare(query).asList(fetchOptions);
    }
    // pull out the parent from each one, retrieve the entries, put them
    // in the required order and deliver
    List<Key> entitiesToGet = new ArrayList<Key>(results.size());
    for (Entity e : results) {
      entitiesToGet.add(e.getParent());
    }
    Map<Key, Entity> unsortedEntities = null;
    try {
      if (txn != null) {
        unsortedEntities = mindashDatastore.get(txn, entitiesToGet);
      } else {
        unsortedEntities = mindashDatastore.get(entitiesToGet);
      }
    } catch (EntityCorruptException e1) {
      // TODO don't know how to handle this yet
      e1.printStackTrace();
    }
    // created sorted list
    List<Entity> sortedEntities =
        new ArrayList<Entity>(unsortedEntities.size());
    for (Key k : entitiesToGet) {
      sortedEntities.add(unsortedEntities.get(k));
    }
    return sortedEntities;
  }

}
