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
import com.google.inject.Singleton;
import com.mindash.datastore.DatastoreHelper;

/**
 * @author Tristan Slominski
 *
 */
@Singleton
public class DatastoreHelperImpl implements DatastoreHelper {

  public void delete(DatastoreService datastore, List<Key> keys) {
    delete(null, datastore, keys);
  }
  
  public void delete(Transaction txn, DatastoreService datastore, 
      List<Key> keys){
    
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
        if (txn != null){
          datastore.delete(txn, deleteChunk);
        } else {
          datastore.delete(deleteChunk);
        }
        index = index + 500;
      }
    } else {
      if (txn != null){
        datastore.delete(txn, keys);
      } else {
        datastore.delete(keys);
      }

    }
    
  }

  public Map<Key, Entity> get(DatastoreService datastore, List<Key> keys) {
    return get(null, datastore, keys);
  }
  
  public Map<Key, Entity> get(Transaction txn, DatastoreService datastore,
      List<Key> keys){
    
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
        Map<Key,Entity> chunk = null;
        if ( txn != null){
          chunk = datastore.get(txn, retrieveChunk);
        } else {
          chunk = datastore.get(retrieveChunk);
        }
        if ( result == null ){
          result = chunk;
        } else {
          result.putAll(chunk);
        }
        index = index + 1000;
      }
    } else {
      if ( txn != null ){
        result = datastore.get(txn, keys);
      } else {
        result = datastore.get(keys);
      }
    }

    return result;
    
  }

  public List<Key> put(DatastoreService datastore, List<Entity> entities) {
    return put(null, datastore, entities);
  }
  
  public List<Key> put(Transaction txn, DatastoreService datastore,
      List<Entity> entities){
   
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
        List<Key> chunk = null;
        if ( txn != null ){
          chunk = datastore.put(txn,commitChunk);
        } else {
          chunk = datastore.put(commitChunk);
        }
        if ( result == null ){
          result = chunk;
        } else {
          result.addAll(chunk);
        }
        index = index + 500;
      }
    } else {
      if ( txn != null ){
        result = datastore.put(txn, entities);
      } else {
        result = datastore.put(entities);
      }
    }
    
//    if ( result.size() != entities.size() ){
//      result = datastore.put(entities);
//    }
    
    return result;
    
  }
}
