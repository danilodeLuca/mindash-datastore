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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * The implementation of {@link com.mindash.datastore.MindashDatastoreService}.
 * 
 * <p>Note: Using the Google Protocol Buffers to generate entities instead of
 * dealing with the <code>Entity</code> interface would probably be better.</p> 
 * 
 * @author Tristan Slominski
 */
public class MindashDatastoreServiceImpl implements MindashDatastoreService {
  
  public MindashDatastoreServiceImpl(){}
  
  /**
   * Instantiates the datastore only once via the factory per documentation.
   */
  private DatastoreService datastore = 
      DatastoreServiceFactory.getDatastoreService();
  
  /**
   * Adds the property overhead for an entity property
   * @param property the property to estimate overhead for
   * @return overhead in bytes
   */
  private long propertyOverheadSize(Entry<String, Object> property) {
    // assumed size of property overhead & size of the key
    return MindashDatastoreService.MindashAssumedPropertyOverhead +
        property.getKey().length() * 4; // allow for UTF-32;
  }
  
  /**
   * Utility method to create key name based on the desired shard.
   * 
   * @param thisShard
   */
  private String createMindashDatastoreKeyName(int thisShard) {
    return MindashDatastoreService.MindashNamePrefixLabel + thisShard;
  }
  
  /**
   * Utility method to create an entity with the appropriate key for the
   * particular shard.
   * @param parentKey the parent key
   * @param thisShard the number of the shard (they start at 0)
   * @return the created entity
   */
  private Entity createMindashEntityShard(Key parentKey, int thisShard) {
    return new Entity(
        MindashDatastoreService.MindashKindLayerLabel,
        this.createMindashDatastoreKeyName(thisShard),
        parentKey);
  }
  
  /**
   * Creates a storable shard that is less than 1MB while consuming properties
   * from the property map.
   * @param properties the map of properties of the main entity
   * @param shard the shard to add the properties to
   * @return shard to store
   */
  private Entity generateStorableEntityShard(Map<String, Object> properties, 
          Entity shard) {
    Iterator<Entry<String,Object>> i = properties.entrySet().iterator();
    long size = MindashDatastoreService.MindashInitialEntityOverheadSize;
    while (i.hasNext()){
      // get the next property
      Entry<String, Object> property = i.next();
      Object value = property.getValue();
      // find out the property's size
      if ( String.class.isInstance(value)){
        // property is a string
        // make sure it is not too long
        if (((String) value).length() > 
            DataTypeUtils.MAX_STRING_PROPERTY_LENGTH){
          throw new IllegalArgumentException("String cannot be longer than " +
              DataTypeUtils.MAX_STRING_PROPERTY_LENGTH);
        }
        // size of the string itself
        size += ((String) value).length() * 4; // allow for UTF-32
        size += propertyOverheadSize(property);
        // see if there is room to add the property
        if ( size < MindashDatastoreService.MindashEntityMaximumSize ){
          // entity can accept this property
          shard.setProperty(property.getKey(), value);
          // remove the property so we don't go to it on the next iteration
          properties.remove(property);
        } else {
          // entity is full, should be closed and a new entity started
          return shard;
        }
      } else if ( double.class.isInstance(value) ){
        // if it can be cast into a double, it should be a number
        // property is a number
        // size is max of 8 bytes
        size += 8;
        size += propertyOverheadSize(property);
        // see if there is room to add the property
        if ( size < MindashDatastoreService.MindashEntityMaximumSize ){
          // entity can accept this property
          shard.setProperty(property.getKey(), value);
          // remove the property so we don't go to it on the next iteration
          properties.remove(property);
        } else {
          // entity is full, should be closed and a new entity sharted
          return shard;
        }
      } else if ( Key.class.isInstance(value) ){
        // currently don't know how big a Key can be (it's recursive so in
        // theory it could be quite large ...
        // property is a Key
        // TODO: determine what size to add
        //      *if Key is unbounded, need to rework the mechanics as we
        //       may have to split the key like a Text or a Blob
        size += propertyOverheadSize(property);
        // see if there is room to add the property
        if ( size < MindashDatastoreService.MindashEntityMaximumSize ){
          // entity can accept this property
          shard.setProperty(property.getKey(), value);
          // remove the property so we don't go to it on the next iteration
          properties.remove(property);
        } else {
          // entity is full, should be closed and a new entity started
          return shard;
        }
      }
      // TODO: implement the other datatypes
    }
    return shard;
  }



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
    int thisShard = 0;
    Key mdKey = KeyFactory.createKey(
        key, 
        MindashDatastoreService.MindashKindLayerLabel,
        this.createMindashDatastoreKeyName(thisShard));
    return datastore.get(mdKey);
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
    /**
     * SOME NOTES:
     * entity will have indexable and non-indexable properties. The indexable
     * properties have maximum sizes, so they can be dealt with in a manner
     * different from really large Blob and Text fields. How do I tell what type
     * of object any given property is in a given entity? (Solution could
     * involve passing in MindashEntity property type along with the object to
     * give a hint as to how to split up entities (by property-value pairs, or
     * breaking up large blob properties))
     */
    Key parentKey = null;
    // check if the key is complete
    if (entity.getKey().getId() != 0 || entity.getKey().getName() != null){
      parentKey = entity.getKey();
    } else {
      // "strip" the entity just to get a parent key (create a tempEntity that
      // will get the key the entity would get if it was saved), this is
      // necessary because if entity > 1MB, when we attempt to
      // datastore.put(entity) it would result in an exception, this way
      // datastore.put(tempEntity) will succeed unless something is horribly
      // wrong
      Entity tempEntity = new Entity(entity.getKind());
      Transaction txn = datastore.beginTransaction();
      parentKey = datastore.put(tempEntity);
      txn.commit();
      // clean up after ourselves and get rid of the placeholder entity so that
      // if the user calls datastore.get on the returned key bypassing
      // MindashDatastoreService, they will get nothing (this is to maintain
      // integrity of the program)
      txn = datastore.beginTransaction();
      datastore.delete(parentKey);
      txn.commit();
    }
    /**
     * As per javadoc, the following are the classes that can be safely stored
     * as properties in the datastore.
     * - String (but not StringBuffer) -> 
     *     limited by DataTypeUtils.MAX_STRING_PROPERTY_LENGTH (500)
     * - from Byte to Long, Float, and Double -> 8 bytes max
     * - Key -> ? size (could be quite large depending on the nesting level)
     * - User -> ? size (should have a constantish size)
     * - ShortBlob (indexable) ->
     *     limited by DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH (500)
     * - Date -> ? size (assuming 8 bytes to store the Long)
     * - Link -> limited by DataTypeUtils.MAX_LINK_PROPERTY_LENGTH (2038)
     * - Blob (unindexed) -> unlimited
     * - Text (unindexed) -> unlimited
     * ********************
     * This is an incarnation of the packing problem: have properties of 
     * different size trying to put them in containers; if i remember correctly,
     * trying to find an optimal arrangement would result in combinatorial 
     * explosion. The naive, and at the same time computationally safe method, 
     * is to walk through the properties map, checking each property and 
     * assembling a storable entity while keeping track of potential size limit.
     * Once we reach the point where adding the next property would make the 
     * entity too large, we just start another entity.
     */
    Map<String, Object> properties = entity.getProperties();
    ArrayList<Entity> shardsToStore = new ArrayList<Entity>();
    // first shard is always 0
    int thisShard = 0;
    while (!properties.isEmpty()){
      Entity shard = createMindashEntityShard(parentKey, thisShard);
      // fill up this shard with properties and add it to storage queue
      shardsToStore.add(generateStorableEntityShard(properties, shard));
      thisShard++;
    }
    // find out how many shards we got
    int shardCount = shardsToStore.size();
    // store the count in the first shard
    shardsToStore.get(0).setProperty(
        MindashDatastoreService.MindashShardCountLabel, shardCount);
    // TODO: do a 500 limit safe put
    datastore.put(shardsToStore);
    // TODO: verify the shards got put
    return parentKey;
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
