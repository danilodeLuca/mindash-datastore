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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.users.User;
import com.google.inject.Inject;

/**
 * The implementation of {@link com.mindash.datastore.MindashDatastoreService}.
 * 
 * <p>Note: Using the Google Protocol Buffers to generate entities instead of
 * dealing with the <code>Entity</code> interface would probably be better.</p> 
 * 
 * @author Tristan Slominski
 */
public class MindashDatastoreServiceImpl implements MindashDatastoreService {
  
  private DatastoreService datastore;
  private DatastoreHelper datastoreHelper;
  
  @Inject
  public MindashDatastoreServiceImpl(DatastoreService datastore,
      DatastoreHelper datastoreHelper){
    this.datastore = datastore;
    this.datastoreHelper = datastoreHelper;
  }
  
  /**
   * Returns the property overhead for an entity property
   * @param property the property to estimate overhead for
   * @return overhead in bytes
   */
  public int getPropertyOverheadSize(Entry<String, Object> property) {
    // assumed size of property overhead & size of the key
    return MindashDatastoreService.MindashAssumedPropertyOverhead +
        property.getKey().length() * 4; // allow for UTF-32;
  }
  
  /**
   * Utility method to create key name based on the desired shard.
   * 
   * @param thisShard
   */
  public static String createMindashDatastoreKeyName(int thisShard) {
    return MindashDatastoreService.MindashNamePrefixLabel + thisShard;
  }
  
  /**
   * Utility method to create a key based on the desired shard.
   * @param key the key to generate Mindash Datastore key for
   * @param shard the shard to get
   * @return the key callable from original DatastoreService
   */
  public static Key createMindashDatastoreKey(Key key, int shard){
    return KeyFactory.createKey(
        key,
        MindashDatastoreService.MindashKindLayerLabel,
        createMindashDatastoreKeyName(shard));
  }
  
  /**
   * Utility method to create an entity with the appropriate key for the
   * particular shard.
   * @param parentKey the parent key
   * @param thisShard the number of the shard (they start at 0)
   * @return the created entity
   */
  public Entity createMindashEntityShard(Key parentKey, int thisShard) {
    return new Entity(
        MindashDatastoreService.MindashKindLayerLabel,
        MindashDatastoreServiceImpl.createMindashDatastoreKeyName(thisShard),
        parentKey);
  }
  
  /**
   * Creates a storable shard that is less than 1MB while consuming properties
   * from the property map.
   * @param entity the original entity with all properties; properties will be
   * stripped from this entity as they are sharded
   * @param shard the shard to add the properties to
   * @return shard to store
   */
  public Entity generateStorableEntityShard(Entity entity, Entity shard) {
    Map<String, Object> properties = entity.getProperties();
    
    /** if there are no properties, return the shard */
    if ( properties == null ){
      return shard;
    }
    
    /** have properties to do things with */
    Iterator<Entry<String,Object>> i = properties.entrySet().iterator();
    long size = MindashDatastoreService.MindashInitialEntityOverheadSize;
    while (i.hasNext()){
      // get the next property
      Entry<String, Object> property = i.next();
      int propertyMaximumSize = 
          MindashDatastoreService.MindashEntityMaximumSize -
          MindashDatastoreService.MindashInitialEntityOverheadSize -
          getPropertyOverheadSize(property);
      Object value = property.getValue();
      // if it's a blob or text, the treatment is different than others
      if ( value instanceof Blob || value instanceof Text ){
        if ( value instanceof Blob){
          // property is a blob
          // blob could easily exceed maximum property size
          if (((Blob)value).getBytes().length > propertyMaximumSize){
            // blob is too big to fit into one property
            // need to split it by splitting the blob from the front,
            // property name will remain the same for get concatenation
            byte[] blobHeadBytes = Arrays.copyOf(((Blob)value).getBytes(), 
                propertyMaximumSize);
            byte[] blobTailBytes = Arrays.copyOfRange(((Blob)value).getBytes(),
                propertyMaximumSize, ((Blob)value).getBytes().length);
            Blob blobHead = new Blob(blobHeadBytes);
            Blob blobTail = new Blob(blobTailBytes);
            size += propertyMaximumSize;
            size += getPropertyOverheadSize(property);
            // see if there is room to add the property
            if ( size <= MindashDatastoreService.MindashEntityMaximumSize){
              // entity can accept the property
              shard.setProperty(property.getKey(), blobHead);
              // replace the remainder of the property with blobTail
              entity.setProperty(property.getKey(), blobTail);
            }
            // entity is full either way
            return shard;
          } else {
            // blob can fit into one property, do the normal thing
            size += ((Blob) value).getBytes().length;
            size += getPropertyOverheadSize(property);
            // see if there is room to add the property
            if ( size <= MindashDatastoreService.MindashEntityMaximumSize ){
              // entity can accept this property
              shard.setProperty(property.getKey(), value);
              // remove the property so we don't go to it on the next iteration
              entity.removeProperty(property.getKey());
              // close the shard with only the blob tail or a blob that fits
              // so that retrieval in getKey can be simplified
              return shard;
            } else {
              // entity is full, should be closed and a new entity started
              return shard;
            }
          }
        } else if ( value instanceof Text ){
          throw new IllegalArgumentException("Mindash Datastore does not " +
          		"support Text type (use Blob instead)");
        }
      } else {
        // find out the property's size
        if ( value instanceof String){
          // property is a string
          // make sure it is not too long
          if (((String) value).length() > 
              DataTypeUtils.MAX_STRING_PROPERTY_LENGTH){
            throw new IllegalArgumentException("String cannot be longer than " +
                DataTypeUtils.MAX_STRING_PROPERTY_LENGTH);
          }
          // size of the string itself
          size += ((String) value).length() * 4; // allow for UTF-32
          size += getPropertyOverheadSize(property);
        } else if ( value instanceof Number || value instanceof Date){
          // property is a number or date (long)
          // size is max of 8 bytes
          size += 8;
          size += getPropertyOverheadSize(property);
        } else if ( value instanceof Key){
          // property is a Key
          // according to Jason from Google Entities can have up to 100 elements
          // in the path and kind and key names can be up to 500 bytes,
          // so theoretical limit for key size is 100,000 bytes + overhead
          // for each recursion.
          // we will use the KeyFactory.keyToString function to hack
          // a safe estimate for storing the key
          size += KeyFactory.keyToString((Key) value).length() * 4; // UTF-32
          size += getPropertyOverheadSize(property);
        } else if ( value instanceof User){
          // property is a User
          // estimating the size of user by getting the length of
          // domain, email, and nickname and adding together
          size += ((User)value).getAuthDomain().length() * 4 + 
          ((User)value).getEmail().length() * 4 +
          ((User)value).getNickname().length() * 4; // allow for UTF-32
          size += getPropertyOverheadSize(property);
        } else if ( value instanceof ShortBlob){
          // property is a shortBlob
          // make sure it is not too long
          if (((ShortBlob) value).getBytes().length >
          DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH){
            throw new IllegalArgumentException("ShortBlog cannot be longer than"
                    + DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH);
          }
          // size of the shortBlob
          size += ((ShortBlob)value).getBytes().length;
          size += getPropertyOverheadSize(property);
        } else if ( value instanceof Link){
          // property is a link
          // make sure it is not too long
          if (((Link)value).getValue().length() >
          DataTypeUtils.MAX_LINK_PROPERTY_LENGTH){
            throw new IllegalArgumentException("Link cannot be longer than" +
                    DataTypeUtils.MAX_LINK_PROPERTY_LENGTH);
          }
          // size of the link
          size += ((Link)value).getValue().length() * 4; // allow for UTF-32
          size += getPropertyOverheadSize(property);
        }
        // see if there is room to add the property
        if ( size <= MindashDatastoreService.MindashEntityMaximumSize ){
          // entity can accept this property
          shard.setProperty(property.getKey(), value);
          // remove the property so we don't go to it on the next iteration
          entity.removeProperty(property.getKey());
        } else {
          // entity is full, should be closed and a new entity started
          return shard;
        }
      }
    }
    return shard;
  }



  public Transaction beginTransaction() {
    return datastore.beginTransaction();
  }

  public void delete(Key... keys) {
    
    delete(null, keys);
    
  }

  /**
   * This utility method calls the datastore to get all 0th shards and then
   * creates all keys to be acted on.
   * @param key0thShards the list of 0th shards
   * @return the list of all shards associated with passed in 0th shards
   */
  private List<Key> generateShardsToDelete(List<Key> key0thShards) {
    
    Map<Key,Entity> shards0th = datastoreHelper.get(datastore, key0thShards);
    
    // for each shard generate keys to be deleted
    List<Key> shardsToDelete = new ArrayList<Key>(shards0th.size());
    for ( Key k : key0thShards){
      Entity e = shards0th.get(k);
      if ( e != null ){
        int shardCount = (Integer)
            e.getProperty(MindashDatastoreService.MindashShardCountLabel);
        for ( int i = 0; i < shardCount; i++){
          shardsToDelete.add(createMindashDatastoreKey(k.getParent(), i));
        }
      }
    }
    return shardsToDelete;
  }

  public void delete(Transaction txn, Key... keys) {
    // constructing keys is more efficient than running multiple queries,
    // so the way delete works, it creates a list of keys to 0 shards of
    // all the entries, it then reads those and creates all the keys to
    // be deleted
    
    List<Key> key0thShards = new ArrayList<Key>(keys.length);
    for (int i = 0; i < keys.length; i++){
      Key k = keys[i];
      if ( k == null ){
        continue;
      }
      key0thShards.add(createMindashDatastoreKey(k, 0));
    }
    
    datastoreHelper.delete(txn, datastore, 
        generateShardsToDelete(key0thShards));
    
  }

  public void delete(Transaction txn, Iterable<Key> keys) {
    // constructing keys is more efficient than running multiple queries,
    // so the way delete works, it creates a list of keys to 0 shards of
    // all the entries, it then reads those and creates all the keys to
    // be deleted
    
    List<Key> key0thShards = new ArrayList<Key>();
    Iterator<Key> iterator = keys.iterator();
    while (iterator.hasNext()){
      Key k = iterator.next();
      key0thShards.add(createMindashDatastoreKey(k, 0));
    }

    datastoreHelper.delete(txn, datastore,
        generateShardsToDelete(key0thShards));
  }

  public void delete(Iterable<Key> keys) {

    delete(null, keys);
    
  }

  public Entity get(Key key) throws EntityNotFoundException, 
      EntityCorruptException {
    
    return get(null, key);
    
  }

  /**
   * @param result the Entity to assemble
   * @param mdKeys the keys of shards to use in assembly
   * @return the assembled <code>result</code> 
   * @throws EntityCorruptException
   */
  private Entity assembleEntity(Transaction txn, Entity result, List<Key> mdKeys)
      throws EntityCorruptException {
    // get the shards
    Map<Key, Entity> shards = null;
    if ( txn != null ){
      shards = datastore.get(txn, mdKeys);
    } else {
      shards = datastore.get(mdKeys);
    }
    // assemble a single entity from the shards
    // map may not be in order so iterate through keys we created
    for (int i = 0; i < mdKeys.size(); i++){
      Entity shard = shards.get(mdKeys.get(i));
      // make sure we got the entity, if not, go get it again
      shard = checkIfNullAndAttemptRetrieval(txn, mdKeys.get(i), shard);
      // if there are more than one property, we can safely assume
      // that there are no split properties
      if ( hasMultipleProperties(shard)){
        result.setPropertiesFrom(shard);
      } else {
        // there will only ever be one split property to start with
        // if this isn't just a single last non-split property
        String propertyName = shard.getProperties().entrySet().iterator()
            .next().getKey();
        Object obj = shard.getProperty(propertyName);
        // get the next shard
        i++;
        // TODO: handle the corner case when an entity is split between
        //       shard 999 and shard 1000
        if ( i >= mdKeys.size() ){
          // no more entities, set the property and return
          result.setProperty(propertyName, obj);
          return result;
        }
        // check if the head of the blob is already in the result
        // this is a corner case of shard 999 and shard 1000 blob split
        obj = result.getProperty(propertyName);
        if ( obj != null ){
          Blob blob = (Blob) obj;
          Blob tail = (Blob) shard.getProperty(propertyName);
          blob = concatenateBlob(blob, tail);
          obj = blob;
          Entity next = shards.get(mdKeys.get(i));
          next = checkIfNullAndAttemptRetrieval(txn, mdKeys.get(i), next);
          // see if next shard has the same property (tail)
          while ( next.hasProperty(propertyName)){
            // yes, found same property, part of the tail!
            // this means that our obj is a blob that was split
            // only blobs get split (otherwise how do you tell if it's a 
            // Text or a Blob? more fields, we don't like that)
            tail = (Blob) next.getProperty(propertyName);
            blob = concatenateBlob(blob, tail);
            // see if there are multiple properties
//            if ( hasMultipleProperties(next) ){
//              // since there are many properties, this is the final tail
//              // set the property
//              result.setProperty(propertyName, obj);
//              // go on to the rest of the properties
//              // remove the property to prevent overwriting the blob
//              next.removeProperty(propertyName);
//              // set the other properties
//              result.setPropertiesFrom(next);
//              // go on to the next shard
//              i++;
//              if ( i >= mdKeys.size() ){
//                return result;
//              }
//            } else {
              // go on to the next shard
              i++;
              if ( i >= mdKeys.size() ){
                // no more entities, set the property and return
                result.setProperty(propertyName, blob);
                return result;
              }
              next = shards.get(mdKeys.get(i));
              next = checkIfNullAndAttemptRetrieval(txn, mdKeys.get(i), next);
            //}
          }
        } else {
          Entity next = shards.get(mdKeys.get(i));
          next = checkIfNullAndAttemptRetrieval(txn, mdKeys.get(i), next);
          // see if next shard has the same property (tail)
          while ( next.hasProperty(propertyName)){
            // yes, found same property, part of the tail!
            // this means that our obj is a blob that was split
            // only blobs get split (otherwise how do you tell if it's a 
            // Text or a Blob? more fields, we don't like that)
            Blob blob = (Blob) obj;
            Blob tail = (Blob) next.getProperty(propertyName);
            blob = concatenateBlob(blob, tail);
            // see if there are multiple properties
            if ( hasMultipleProperties(next) ){
              // since there are many properties, this is the final tail
              // set the property
              result.setProperty(propertyName, blob);
              // go on to the rest of the properties
              // remove the property to prevent overwriting the blob
              next.removeProperty(propertyName);
              // set the other properties
              result.setPropertiesFrom(next);
              // go on to the next shard
              i++;
              if ( i >= mdKeys.size() ){
                return result;
              }
            } else {
              // go on to the next shard
              i++;
              if ( i >= mdKeys.size() ){
                // no more entities, set the property and return
                result.setProperty(propertyName, blob);
                return result;
              }
              next = shards.get(mdKeys.get(i++));
              next = checkIfNullAndAttemptRetrieval(txn, mdKeys.get(i), next);
              obj = blob;
            }
          }
        }    
        // next did not have the same property and next exists
        // commit the object we were suspecting of being a split object
        // or if we just put together a blob
        result.setProperty(propertyName, obj);
        i--; // reset the count and let the loop do its thing
      }
    }
    return result;
  }

  /**
   * @param key
   * @param shard
   * @return
   * @throws EntityCorruptException
   */
  private Entity checkIfNullAndAttemptRetrieval(Transaction txn, Key key,
      Entity shard)
          throws EntityCorruptException {
    if ( shard == null ){
      try {
        if ( txn != null ){
          shard = datastore.get(txn, key);
        } else {
          shard = datastore.get(key);
        }
      } catch (EntityNotFoundException ex){
        // this is very bad, we lost the integrity of the data
        // let the user know
        throw new EntityCorruptException("One of the Entity shards was not " +
        		"found. The entity is corrupt and cannot be retrieved");
      }
    }
    return shard;
  }

  /**
   * Utility method to concatenate two blobs.
   * @param head 
   * @param tail
   */
  public Blob concatenateBlob(Blob head, Blob tail) {
    byte[] newBlob = Arrays.copyOf(head.getBytes(), 
        head.getBytes().length + tail.getBytes().length);
    System.arraycopy(tail.getBytes(), 0, newBlob, head.getBytes().length,
        tail.getBytes().length);
    // newBlob should now be concatenation of blob and tail
    return new Blob(newBlob);
  }

  /**
   * Utility method to determine if there are multiple properties in a given
   * entity (discounting MindashShardCountLabel)
   * @param entity the entity to test
   * @return <code>true</code> if <code>entity</code> has multiple properties
   */
  private boolean hasMultipleProperties(Entity entity) {
    //remember shard 0 has an extra MindashShardCountLabel property
    return (entity.hasProperty(MindashDatastoreService.MindashShardCountLabel)
        && entity.getProperties().size() > 2) || 
        (entity.getProperties().size() > 1 );
  }

  /**
   * Uses reflection to call private Entity(Key key) constructor.
   * @param key the key to use
   * @return the constructed entity
   */
  @SuppressWarnings("unchecked")
  private Entity constructEntity(Key key) {
    Class[] argClasses = new Class[1];
    argClasses[0] = Key.class;
    Object[] argObjects = new Object[1];
    argObjects[0] = key;
    Entity result = null;
    try {
      result = (Entity)
          MindashDatastoreServiceImpl.invokeConstructor(
              Entity.class, argClasses, argObjects);
    } catch (InvocationTargetException e1) {
      // TODO probably send this to the logger
      e1.printStackTrace();
    }
    return result;
  }

  public Entity get(Transaction txn, Key key) throws EntityNotFoundException,
      EntityCorruptException {
    int thisShard = 0;
    Key mdKey = createMindashDatastoreKey(key, thisShard);
    Entity e = null;
    if ( txn != null ){
      e = datastore.get(txn, mdKey);
    } else {
      e = datastore.get(mdKey);
    }
    // got the 0th shard
    // check how many shards there are
    int shardCount = (Integer) e.getProperty(
        MindashDatastoreService.MindashShardCountLabel);
    // create the result entity using the passed key
    Entity result = constructEntity(key);
    if ( shardCount > 1 ){
      // get the other shards
      // create the keys
      List<Key> mdKeys = new ArrayList<Key>(shardCount);
      for ( int i = 0; i < shardCount; i++){
        mdKeys.add(createMindashDatastoreKey(key, i));
      }
      // account for more than 1000 shards
      if ( mdKeys.size() > 1000 ){
        List<Key> retrieveChunk = null;
        int index = 0;
        int indexHigh = 0;
        while ( index < mdKeys.size() ){
          indexHigh = index + 999;
          if ( indexHigh >= mdKeys.size() ){
            indexHigh = mdKeys.size() - 1;
          }
          retrieveChunk = mdKeys.subList(index, indexHigh + 1);
          result = assembleEntity(txn, result, retrieveChunk);
          index = index + 1000;
        }
      } else {
        result = assembleEntity(txn, result, mdKeys);
      }
    } else {
      // only one shard
      result.setPropertiesFrom(e);
    }
    result.removeProperty(MindashDatastoreService.MindashShardCountLabel);
    return result;
  }

  public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) {
    throw new NotImplementedException();
  }

  public Map<Key, Entity> get(Iterable<Key> keys) {
    throw new NotImplementedException();
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
    throw new NotImplementedException();
  }

  public PreparedQuery prepare(Transaction txn, Query query) {
    throw new NotImplementedException();
  }

  public Key put(Entity entity) {
    
    return put(null, entity);
    
  }

  public Key put(Transaction txn, Entity entity) {
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
      //Transaction txn = datastore.beginTransaction();
      if ( txn != null ){
        parentKey = datastore.put(txn, tempEntity);
      } else {
        parentKey = datastore.put(tempEntity);       
      }
      //txn.commit();
      // clean up after ourselves and get rid of the placeholder entity so that
      // if the user calls datastore.get on the returned key bypassing
      // MindashDatastoreService, they will get nothing (this is to maintain
      // integrity of the program)
      if ( txn != null ){
        datastore.delete(txn, parentKey);
      } else {
        datastore.delete(parentKey);
      }
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
    ArrayList<Entity> shardsToStore = new ArrayList<Entity>();
    // first shard is always 0
    int thisShard = 0;
    while (true){
      Entity shard = createMindashEntityShard(parentKey, thisShard);
      // fill up this shard with properties and add it to storage queue
      shardsToStore.add(generateStorableEntityShard(entity, shard));
      thisShard++;
      if (entity.getProperties().isEmpty()){
        break;
      }
    }
    // find out how many shards we got
    int shardCount = shardsToStore.size();
    // store the count in the first shard
    shardsToStore.get(0).setProperty(
        MindashDatastoreService.MindashShardCountLabel, shardCount);
    // 500 limit safe put
    datastoreHelper.put(txn, datastore, shardsToStore);
    // TODO: verify the shards got put
    return parentKey;
  }

  public List<Key> put(Transaction txn, Iterable<Entity> entities) {
    throw new NotImplementedException();
  }

  public List<Key> put(Iterable<Entity> entities) {
    throw new NotImplementedException();
  }
  
  @SuppressWarnings("unchecked")
  private static Object invokeConstructor(Class targetClass, Class[] argClasses,
      Object[] argObjects) throws InvocationTargetException {
      Constructor constructor;
      try {
        constructor = targetClass.getDeclaredConstructor(argClasses);
        constructor.setAccessible(true);
        return constructor.newInstance(argObjects);
      } catch (SecurityException e) {
        // Should happen only rarely, because the setAccessible(true)
        // should be allowed in when running unit tests. If it does
        // happen, just let the test fail so the programmer can fix
        // the problem.
        throw new InvocationTargetException(e);
      } catch (NoSuchMethodException e) {
        // Should happen only rarely, because most times the
        // specified method should exist. If it does happen, just let
        // the test fail so the programmer can fix the problem.
        throw new InvocationTargetException(e);
      } catch (IllegalArgumentException e) {
        // Should happen only rarely, because usually the right
        // number and types of arguments will be passed. If it does
        // happen, just let the test fail so the programmer can fix
        // the problem.
        throw new InvocationTargetException(e);
      } catch (InstantiationException e) {
        // Should happen only rarely because this crap should work
        throw new InvocationTargetException(e);
      } catch (IllegalAccessException e) {
        // Should never happen, because setting accessible flag to
        // true. If setting accessible fails, should throw a security
        // exception at that point and never get to the invoke. But
        // just in case, wrap it in a InvocationTargetException and let a
        // human figure it out.
        throw new InvocationTargetException(e);
      }
  }

}
