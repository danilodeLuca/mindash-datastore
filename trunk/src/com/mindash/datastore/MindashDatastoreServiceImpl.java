/**
 * Copyright 2009 Tristan Slominski
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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * The implementation of {@link com.mindash.datastore.MindashDatastoreService}.
 * 
 * @author Tristan Slominski
 */
public class MindashDatastoreServiceImpl implements MindashDatastoreService {
  
  /**
   * Instantiates the datastore only once via the factory per documentation.
   */
  private DatastoreService datastore = 
      DatastoreServiceFactory.getDatastoreService();

  public Transaction beginTransaction() {
    return datastore.beginTransaction();
  }

  public void delete(Key... keys) {
    datastore.delete(keys);
  }

  public void delete(Transaction txn, Key... keys) {
    datastore.delete(txn, keys);
  }

  public void delete(Transaction txn, Iterable<Key> keys) {
    datastore.delete(txn, keys);
  }

  public void delete(Iterable<Key> keys) {
    datastore.delete(keys);
  }

  public Entity get(Key key) throws EntityNotFoundException {
    return datastore.get(key);
  }

  public Entity get(Transaction txn, Key key) throws EntityNotFoundException {
    return datastore.get(txn, key);
  }

  public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) {
    return datastore.get(txn, keys);
  }

  public Map<Key, Entity> get(Iterable<Key> keys) {
    return datastore.get(keys);
  }

  public Collection<Transaction> getActiveTransactions() {
    return datastore.getActiveTransactions();
  }

  public Transaction getCurrentTransaction() {
    return datastore.getCurrentTransaction();
  }

  public Transaction getCurrentTransaction(Transaction txn) {
    return datastore.getCurrentTransaction(txn);
  }

  public PreparedQuery prepare(Query query) {
    return datastore.prepare(query);
  }

  public PreparedQuery prepare(Transaction txn, Query query) {
    return datastore.prepare(txn, query);
  }

  public Key put(Entity entity) {
    return datastore.put(entity);
  }

  public Key put(Transaction txn, Entity entity) {
    return datastore.put(txn, entity);
  }

  public List<Key> put(Transaction txn, Iterable<Entity> entities) {
    return datastore.put(txn, entities);
  }

  public List<Key> put(Iterable<Entity> entities) {
    return datastore.put(entities);
  }

}
