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

import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

/**
 * @author Tristan Slominski
 *
 */
public interface DatastoreHelper {

  public Map<Key,Entity> get(DatastoreService datastore, List<Key> keys);
  public Map<Key,Entity> get(Transaction txn, DatastoreService datastore,
      List<Key> keys);
  public List<Key> put(DatastoreService datastore, List<Entity> entities); 
  public List<Key> put(Transaction txn, DatastoreService datastore,
      List<Entity> entities);
  public void delete(DatastoreService datastore, List<Key> keys);
  public void delete(Transaction txn, DatastoreService datastore, 
      List<Key> keys);
  
}
