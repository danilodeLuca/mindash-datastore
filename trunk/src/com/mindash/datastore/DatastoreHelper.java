/**
 * Copyright 2009 Tristan Slominski
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.
 */
package com.mindash.datastore;

import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

/**
 * @author Tristan Slominski
 *
 */
public interface DatastoreHelper {

  public Map<Key,Entity> get(DatastoreService datastore, List<Key> keys);
  public List<Key> put(DatastoreService datastore, List<Entity> entities);  
  public void delete(DatastoreService datastore, List<Key> keys);
  
}
