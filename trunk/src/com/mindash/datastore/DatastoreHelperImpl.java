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
import com.google.inject.Singleton;
import com.mindash.datastore.DatastoreHelper;

/**
 * @author Tristan Slominski
 *
 */
@Singleton
public class DatastoreHelperImpl implements DatastoreHelper {

  public void delete(DatastoreService datastore, List<Key> keys) {
    
    if (keys.size() > 500) {
      List<Key> deleteChunk = null;
      int index = 0;
      int indexHigh = 0;
      while ( index < keys.size()){
        indexHigh = index + 499;
        if (indexHigh >= keys.size()){
          indexHigh = keys.size() - 1;
        }
        deleteChunk = keys.subList(index, indexHigh + 1);
        datastore.delete(deleteChunk);
        index = index + 500;
      }
    } else {
      datastore.delete(keys);
    }
    
  }

  public Map<Key, Entity> get(DatastoreService datastore, List<Key> keys) {
    
    Map<Key, Entity> result = null;
    if ( keys.size() > 1000 ){
      List<Key> retrieveChunk = null;
      int index = 0;
      int indexHigh = 0;
      while ( index < keys.size() ){
        indexHigh = index + 999;
        if ( indexHigh >= keys.size() ){
          indexHigh = keys.size() - 1;
        }
        retrieveChunk = keys.subList(index, indexHigh + 1);
        Map<Key, Entity> chunk = datastore.get(retrieveChunk);
        if ( result == null ){
          result = chunk;
        } else {
          result.putAll(chunk);
        }
        index = index + 1000;
      }
    } else {
      result = datastore.get(keys);
    }

    return result;
    
  }

  public List<Key> put(DatastoreService datastore, List<Entity> entities) {

    List<Key> result = null;
    
    if (entities.size() > 500) {
      List<Entity> commitChunk = null;
      int index = 0;
      int indexHigh = 0;
      while ( index < entities.size()){
        indexHigh = index + 499;
        if (indexHigh >= entities.size()){
          indexHigh = entities.size() - 1;
        }
        commitChunk = entities.subList(index, indexHigh + 1);
        List<Key> chunk = datastore.put(commitChunk);
        if ( result == null ){
          result = chunk;
        } else {
          result.addAll(chunk);
        }
        index = index + 500;
      }
    } else {
      result = datastore.put(entities);
    }
    
//    if ( result.size() != entities.size() ){
//      result = datastore.put(entities);
//    }
    
    return result;
    
  }
}
