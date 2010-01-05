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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.createNiceMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import atunit.AtUnit;
import atunit.Container;
import atunit.Mock;
import atunit.MockFramework;
import atunit.Unit;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.users.User;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.mindash.datastore.DatastoreHelper;
import com.mindash.datastore.EntityCorruptException;
import com.mindash.datastore.LocalDatastoreTestCase;
import com.mindash.datastore.MindashDatastoreService;
import com.mindash.util.EntityWithIncompleteKeyEquals;
import com.mindash.util.ListOf0thShardKeysEquals;
import com.mindash.util.ListOfEntitiesSizeEquals;
import com.mindash.util.ListOfEntitiesWithMddLayerEquals;

/**
 * Test for <code>MindashDatastoreServiceImpl</code>
 * 
 * @author Tristan Slominski
 * 
 */
@RunWith(AtUnit.class)
@MockFramework(MockFramework.Option.EASYMOCK)
@Container(Container.Option.GUICE)
public class MindashDatastoreServiceImplTest extends LocalDatastoreTestCase
    implements Module {

  @Inject
  Injector injector;
  @Inject
  Logger logger;

  @Inject
  @Unit
  MindashDatastoreService md;

  @Mock
  Transaction transaction;

  @Inject
  DatastoreService datastore;

  MindashDatastoreServiceImpl mdImpl;

  @Override
  public void configure(Binder b) {
    /*
     * Class bindings
     */
    b.bind(DatastoreHelper.class).to(DatastoreHelperImpl.class);
    b.bind(DatastoreService.class).toInstance(
        createNiceMock(DatastoreService.class));
    b.bind(MindashDatastoreService.class).to(MindashDatastoreServiceImpl.class);
  }

  /**
   * @param bufferLength the length of buffer to generate
   * @return the buffer of specified length
   */
  private byte[] generateByteBuffer(int bufferLength) {
    String fileName = "semantics.pdf";
    String path = "test/com/mindash/datastore/" + fileName;
    File book = new File(path);
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(book);
    } catch (FileNotFoundException e1) {
      fail("File \"" + fileName + "\" not found at \"" + path + "\"");
    }
    byte[] buffer = new byte[bufferLength];
    try {
      fis.read(buffer);
    } catch (IOException e1) {
      fail("IOException!");
    }
    return buffer;
  }

  @Before
  public void setUp() {
    super.setUp();
    mdImpl = (MindashDatastoreServiceImpl) md;
    // when(datastore.beginTransaction()).thenReturn(mock(Transaction.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getPropertyOverheadSizeShouldReturnProperPropertyOverheadSize() {
    Entry<String, Object> property = createMock(Entry.class);
    expect(property.getKey()).andReturn("testPropertyName");
    replay(property);
    int result = mdImpl.getPropertyOverheadSize(property);
    assertTrue("Property overhead size should be the sum of "
        + "MindashAssumedPropertyOverhead + length of property name * 4",
        result == MindashDatastoreService.MindashAssumedPropertyOverhead
            + "testPropertyName".length() * 4);
  }

  @Test
  public void createMindashDatastoreKeyNameShouldReturnConcatenationOfNamePrefixLabelAndShardNumber() {
    int shardNum = 3;
    String result =
        MindashDatastoreServiceImpl.createMindashDatastoreKeyName(shardNum);
    assertTrue("MindashDatastoreKeyName should be a concatenation of "
        + "MindashNamePrefixLabel and shard number", result
        .equals(MindashDatastoreService.MindashNamePrefixLabel + 3));
  }

  @Test
  public void createMindashDatastoreKeyShouldCreateTheProperKey() {
    Key originalKey = KeyFactory.createKey("testKind", "testKey");
    int shard = 3;
    Key createdKey =
        MindashDatastoreServiceImpl.createMindashDatastoreKey(originalKey,
            shard);
    assertTrue("MindashDatastoreKey should have original key as ancestor",
        createdKey.getParent().equals(originalKey));
    assertTrue("MindashDatastoreKey should have kind of the ancestor",
        createdKey.getKind().equals(originalKey.getKind()));
    assertTrue("MindashDatastoreKey should have name created by "
        + "createMindashDatastoreKeyName()", createdKey.getName().equals(
        MindashDatastoreServiceImpl.createMindashDatastoreKeyName(3)));
  }

  @Test
  public void createMindashEntityShardShouldCreateCorrectShard() {
    Key originalKey = KeyFactory.createKey("testKind", "testKey");
    int shard = 3;
    Entity result = mdImpl.createMindashEntityShard(originalKey, shard);
    assertTrue("Created shard should have kind of the ancestor", result
        .getKind().equals(originalKey.getKind()));
    assertTrue("Created shard should have name created by "
        + "createMindashDatastoreKeyName()", result.getKey().getName().equals(
        MindashDatastoreServiceImpl.createMindashDatastoreKeyName(3)));
    assertTrue("Created shard should have original key as parent", result
        .getKey().getParent().equals(originalKey));
  }

  @Test
  public void generateStorableEntityShardShouldReturnShardWithNoPropertiesIfThereAreNoProperties() {
    Entity entity = new Entity("testKind", "testName");
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have no properties", shard.getProperties()
        .isEmpty());
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneStringProperty() {
    Entity entity = new Entity("testKind", "testName");
    entity.setProperty("property1", "This is a string property");
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one string property", shard
        .getProperty("property1").equals("This is a string property"));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneIntegerProperty() {
    Entity entity = new Entity("testKind", "testName");
    entity.setProperty("property1", 1);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one integer property", shard
        .getProperty("property1").equals(1));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneDoubleProperty() {
    Entity entity = new Entity("testKind", "testName");
    entity.setProperty("property1", (double) 1.0);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one double property", shard
        .getProperty("property1").equals((double) 1.0));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneKeyProperty() {
    Entity entity = new Entity("testKind", "testName");
    Key key = KeyFactory.createKey("testKey", 1234);
    entity.setProperty("property1", key);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one key property", shard.getProperty(
        "property1").equals(key));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneUserProperty() {
    Entity entity = new Entity("testKind", "testName");
    User user = new User("example@email.com", "google.com");
    entity.setProperty("property1", user);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one user property", shard
        .getProperty("property1").equals(user));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneShortBlobProperty() {
    Entity entity = new Entity("testKind", "testName");
    ShortBlob sb = new ShortBlob("This is some short blob".getBytes());
    entity.setProperty("property1", sb);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one short blob property", shard
        .getProperty("property1").equals(sb));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneDateProperty() {
    Entity entity = new Entity("testKind", "testName");
    Date date = new Date();
    entity.setProperty("property1", date);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one date property", shard
        .getProperty("property1").equals(date));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneLinkProperty() {
    Entity entity = new Entity("testKind", "testName");
    Link link = new Link("some link");
    entity.setProperty("property1", link);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one link property", shard
        .getProperty("property1").equals(link));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneBlobPropertyShortLength() {
    Entity entity = new Entity("testKind", "testName");
    Blob blob = new Blob("this is a short sized blob".getBytes());
    entity.setProperty("property1", blob);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one blob property", shard
        .getProperty("property1").equals(blob));
  }

  @Test
  public void generateStorableEntityShardShouldCreateCorrectShardWithOneBlobPropertyLongLength() {
    Entity entity = new Entity("testKind", "testName");

    /* generate really long blob */
    byte[] buffer = generateByteBuffer(2000000);

    Blob blob = new Blob(buffer);
    entity.setProperty("property1", blob);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Created shard should have only one property", shard
        .getProperties().size() == 1);
    assertTrue("Created shard should have one blob property", Blob.class
        .isInstance(shard.getProperty("property1")));
  }

  @Test
  public void generateStorableEntityShardShouldLeaveUncommittedPartOfBlobInTheOriginalEntity() {
    Entity entity = new Entity("testKind", "testName");

    /* generate really long blob */
    byte[] buffer = generateByteBuffer(2000000);

    Blob blob = new Blob(buffer);
    entity.setProperty("property1", blob);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Original entity should have one property remain", entity
        .getProperties().size() == 1);
    assertTrue("Original entity should have a blob property remain", Blob.class
        .isInstance(shard.getProperty("property1")));
    assertTrue("Original entity should have uncommitted part of original blob",
        Arrays.equals(((Blob) entity.getProperty("property1")).getBytes(),
            Arrays.copyOfRange(blob.getBytes(), ((Blob) shard
                .getProperty("property1")).getBytes().length,
                blob.getBytes().length)));
  }

  @Test
  public void generateStorableEntityShardShouldThrowIllegalArgumentExceptionWhenGivenATextProperty() {
    Entity entity = new Entity("testKind", "testName");
    Text text = new Text("sample text");
    entity.setProperty("property1", text);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    try {
      shard = mdImpl.generateStorableEntityShard(entity, shard);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(true);
    }
  }

  @Test
  public void generateStorableEntityShardShouldConsumeCommittedPropertyFromOriginalEntity() {
    Entity entity = new Entity("testKind", "testName");
    String string = "test string";
    entity.setProperty("property1", string);
    Entity shard = mdImpl.createMindashEntityShard(entity.getKey(), 3);
    shard = mdImpl.generateStorableEntityShard(entity, shard);
    assertTrue("Original entity should have no properties", entity
        .getProperties().size() == 0);
  }

  @Test
  public void beginTransactionShouldCallDatastoreBeginTransaction() {
    expect(datastore.beginTransaction()).andReturn(transaction);
    replay(datastore);
    Transaction t = md.beginTransaction();
    verify(datastore);
    assertTrue(t.equals(transaction));
  }

  //
  // // class IsAncestorQuery extends ArgumentMatcher<Query>{
  // // Key ancestor;
  // // public IsAncestorQuery(Key ancestor){
  // // super();
  // // this.ancestor = ancestor;
  // // }
  // // public boolean matches(Object query){
  // // if (Query.class.isInstance(query)){
  // // Query q = (Query) query;
  // // if ( q.getAncestor() != null &&
  // // q.getAncestor().equals(ancestor)){
  // // return true;
  // // }
  // // }
  // // return false;
  // // }
  // // }
  //
  // class IsListOf0thShardKeys extends ArgumentMatcher<List<Key>> {
  // public IsListOf0thShardKeys() {
  // super();
  // }
  //
  // @SuppressWarnings("unchecked")
  // public boolean matches(Object list) {
  // if (List.class.isInstance(list)) {
  // List<Key> l = (List) list;
  // for (Key k : l) {
  // if (!k.getName().equals(
  // MindashDatastoreService.MindashNamePrefixLabel + 0)) {
  // return false;
  // }
  // }
  // return true;
  // }
  // return false;
  // }
  // }

  @Test
  public void deleteKeyOneShouldGet0thShardFromDatastore()
      throws EntityNotFoundException {
    Key key = KeyFactory.createKey("testKind", "testName");
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(new HashMap<Key, Entity>());
    replay(datastore);
    md.delete(key);
    verify(datastore);
  }

  @Test
  public void deleteKeyTwoShouldGet0thShardsFromDatastore() {
    Key key1 = KeyFactory.createKey("testKind1", "testName1");
    Key key2 = KeyFactory.createKey("testKind2", "testName2");
    Entity shard01 = mdImpl.createMindashEntityShard(key1, 0);
    Entity shard02 = mdImpl.createMindashEntityShard(key2, 0);
    Map<Key, Entity> results = new HashMap<Key, Entity>(2);
    results.put(shard01.getKey(), shard01);
    results.put(shard02.getKey(), shard02);
    Key[] keys = new Key[2];
    keys[0] = key1;
    keys[1] = key2;
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(new HashMap<Key, Entity>());
    replay(datastore);
    md.delete(keys);
    verify(datastore);
  }

  @Test
  public void deleteKeyShouldDeleteAllShardsFromDatastore() {
    Key key = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(key, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 3);
    Entity shard1 = mdImpl.createMindashEntityShard(key, 1);
    Entity shard2 = mdImpl.createMindashEntityShard(key, 2);
    Map<Key, Entity> shard0results = new HashMap<Key, Entity>(1);
    shard0results.put(shard0.getKey(), shard0);
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(shard0results);
    List<Key> keys = new ArrayList<Key>(3);
    keys.add(shard0.getKey());
    keys.add(shard1.getKey());
    keys.add(shard2.getKey());
    datastore.delete(keys);
    expectLastCall().once();
    replay(datastore);
    md.delete(key);
    verify(datastore);
  }

  @Test
  public void deleteIterableKeysOneShouldGet0thShardFromDatastore()
      throws EntityNotFoundException {
    Key key = KeyFactory.createKey("testKind", "testName");
    ArrayList<Key> keys = new ArrayList<Key>(1);
    keys.add(key);
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(new HashMap<Key, Entity>());
    replay(datastore);
    md.delete(keys);
    verify(datastore);
  }

  @Test
  public void deleteIterableKeysTwoShouldGet0thShardsFromDatastore() {
    Key key1 = KeyFactory.createKey("testKind1", "testName1");
    Key key2 = KeyFactory.createKey("testKind2", "testName2");
    Entity shard01 = mdImpl.createMindashEntityShard(key1, 0);
    Entity shard02 = mdImpl.createMindashEntityShard(key2, 0);
    Map<Key, Entity> results = new HashMap<Key, Entity>(2);
    results.put(shard01.getKey(), shard01);
    results.put(shard02.getKey(), shard02);
    ArrayList<Key> keys = new ArrayList<Key>(2);
    keys.add(key1);
    keys.add(key2);
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(new HashMap<Key, Entity>());
    replay(datastore);
    md.delete(keys);
    verify(datastore);
  }

  @Test
  public void deleteIterableKeysShouldDeleteAllShardsFromDatastore() {
    Key key = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(key, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 3);
    Entity shard1 = mdImpl.createMindashEntityShard(key, 1);
    Entity shard2 = mdImpl.createMindashEntityShard(key, 2);
    Map<Key, Entity> shard0results = new HashMap<Key, Entity>(1);
    shard0results.put(shard0.getKey(), shard0);
    expect(datastore.get(ListOf0thShardKeysEquals.eqListOf0thShardKeys()))
        .andReturn(shard0results);
    ArrayList<Key> iKey = new ArrayList<Key>(1);
    iKey.add(key);
    List<Key> keys = new ArrayList<Key>(3);
    keys.add(shard0.getKey());
    keys.add(shard1.getKey());
    keys.add(shard2.getKey());
    datastore.delete(keys);
    expectLastCall().once();
    replay(datastore);
    md.delete(iKey);
    verify(datastore);
  }

  // @Test
  // public void testMindashDatastoreServiceImplDeleteTransactionKeys(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplDeleteTransactionIterableKey(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServideImplDeleteIterableKey(){
  // assertTrue("Not implemented", false);
  // }

  @Test
  public void getKeyShouldRetrieve0thShard() throws EntityNotFoundException,
      EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Key googleDatastoreKey =
        MindashDatastoreServiceImpl.createMindashDatastoreKey(mindashKey, 0);
    Entity shard0 = mdImpl.createMindashEntityShard(googleDatastoreKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 1);
    expect(datastore.get(googleDatastoreKey)).andReturn(shard0);
    replay(datastore);
    md.get(mindashKey);
    verify(datastore);
  }

  @Test
  public void getKeyShouldRetrieveAllShardsIfMoreThanOne()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Key shard0Key =
        MindashDatastoreServiceImpl.createMindashDatastoreKey(mindashKey, 0);
    Entity shard0 = mdImpl.createMindashEntityShard(shard0Key, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 3);
    shard0.setProperty("0", "zero");
    expect(datastore.get(shard0Key)).andReturn(shard0);
    Key shard1Key =
        MindashDatastoreServiceImpl.createMindashDatastoreKey(mindashKey, 1);
    Entity shard1 = mdImpl.createMindashEntityShard(shard0Key, 1);
    shard1.setProperty("1", "one");
    Key shard2Key =
        MindashDatastoreServiceImpl.createMindashDatastoreKey(mindashKey, 2);
    Entity shard2 = mdImpl.createMindashEntityShard(shard0Key, 2);
    shard2.setProperty("2", "two");
    List<Key> keys = new ArrayList<Key>(3);
    keys.add(shard0Key);
    keys.add(shard1Key);
    keys.add(shard2Key);
    Map<Key, Entity> shards = new HashMap<Key, Entity>(3);
    shards.put(shard0Key, shard0);
    shards.put(shard1Key, shard1);
    shards.put(shard2Key, shard2);
    expect(datastore.get(keys)).andReturn(shards);
    replay(datastore);
    md.get(mindashKey);
    verify(datastore);
  }

  @Test
  public void getKeyShouldReturnCorrectEntityFromSingleShard()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(mindashKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 1);
    shard0.setProperty("1", "one");
    shard0.setProperty("2", "two");
    expect(datastore.get(shard0.getKey())).andReturn(shard0);
    replay(datastore);
    Entity result = md.get(mindashKey);
    assertTrue("Result should have property '1' with value 'one'", result
        .getProperty("1").equals("one"));
    assertTrue("Result should have property '2' with value 'two'", result
        .getProperty("2").equals("two"));
  }

  @Test
  public void getKeyShouldReturnCorrectEntityFromTwoShards()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(mindashKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 2);
    shard0.setProperty("1", "one");
    Entity shard1 = mdImpl.createMindashEntityShard(mindashKey, 1);
    shard1.setProperty("2", "two");
    List<Key> keys = new ArrayList<Key>(2);
    keys.add(shard0.getKey());
    keys.add(shard1.getKey());
    Map<Key, Entity> shards = new HashMap<Key, Entity>(2);
    shards.put(shard0.getKey(), shard0);
    shards.put(shard1.getKey(), shard1);
    expect(datastore.get(shard0.getKey())).andReturn(shard0);
    expect(datastore.get(keys)).andReturn(shards);
    replay(datastore);
    Entity result = md.get(mindashKey);
    assertTrue("Result should have property '1' with value 'one'", result
        .getProperty("1").equals("one"));
    assertTrue("Result should have property '2' with value 'two'", result
        .getProperty("2").equals("two"));
  }

  @Test
  public void getKeyShouldReturnCorrectEntityFrom1200Shards()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(mindashKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 1200);
    shard0.setProperty("0", 0);
    shard0.setProperty("2000", 2000);
    List<Key> keys = new ArrayList<Key>(1200);
    keys.add(shard0.getKey());
    Map<Key, Entity> first1000Shards = new HashMap<Key, Entity>(1000);
    first1000Shards.put(shard0.getKey(), shard0);
    Map<Key, Entity> last200Shards = new HashMap<Key, Entity>(200);
    for (int i = 1; i < 1000; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(2000 + i), 2000 + i);
      keys.add(shard.getKey());
      first1000Shards.put(shard.getKey(), shard);
    }
    for (int i = 1000; i < 1200; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(2000 + i), 2000 + i);
      keys.add(shard.getKey());
      last200Shards.put(shard.getKey(), shard);
    }
    expect(datastore.get(shard0.getKey())).andReturn(shard0);
    expect(datastore.get(keys.subList(0, 1000))).andReturn(first1000Shards);
    expect(datastore.get(keys.subList(1000, 1200))).andReturn(last200Shards);
    replay(datastore);
    Entity result = md.get(mindashKey);
    for (int i = 0; i < 1200; i++) {
      assertTrue("Result should have property '" + i + "' with value '" + i
          + "'", (Integer) result.getProperty(String.valueOf(i)) == i);
      assertTrue("Result should have property '" + (2000 + i)
          + "' with value '" + (2000 + i) + "'", (Integer) result
          .getProperty(String.valueOf(2000 + i)) == 2000 + i);
    }
  }

  @Test
  public void getKeyShouldReturnCorrectEntityFrom1200ShardsWithBlobSplitBetweenShard999AndShard1000()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(mindashKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 1200);
    shard0.setProperty("0", 0);
    shard0.setProperty("2000", 2000);
    List<Key> keys = new ArrayList<Key>(1200);
    keys.add(shard0.getKey());
    Map<Key, Entity> first1000Shards = new HashMap<Key, Entity>(1000);
    first1000Shards.put(shard0.getKey(), shard0);
    Map<Key, Entity> last200Shards = new HashMap<Key, Entity>(200);
    for (int i = 1; i < 999; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(2000 + i), 2000 + i);
      keys.add(shard.getKey());
      first1000Shards.put(shard.getKey(), shard);
    }
    Entity shard999 = mdImpl.createMindashEntityShard(mindashKey, 999);
    shard999.setProperty("MyBlob", new Blob("blob head".getBytes()));
    keys.add(shard999.getKey());
    first1000Shards.put(shard999.getKey(), shard999);
    Entity shard1000 = mdImpl.createMindashEntityShard(mindashKey, 1000);
    shard1000.setProperty("MyBlob", new Blob("blob tail".getBytes()));
    keys.add(shard1000.getKey());
    last200Shards.put(shard1000.getKey(), shard1000);
    for (int i = 1001; i < 1200; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(2000 + i), 2000 + i);
      keys.add(shard.getKey());
      last200Shards.put(shard.getKey(), shard);
    }
    expect(datastore.get(shard0.getKey())).andReturn(shard0);
    expect(datastore.get(keys.subList(0, 1000))).andReturn(first1000Shards);
    expect(datastore.get(keys.subList(1000, 1200))).andReturn(last200Shards);
    replay(datastore);
    Entity result = md.get(mindashKey);
    assertTrue("Result should have one property 'MyBlob' with value "
        + "'blob headblob tail'", new String(((Blob) result
        .getProperty("MyBlob")).getBytes()).equals("blob headblob tail"));
  }

  @Test
  public void getKeyShouldReturnCorrectEntityFrom2100ShardsWithBlobSplitBetweenShard999AndShard2001()
      throws EntityNotFoundException, EntityCorruptException {
    Key mindashKey = KeyFactory.createKey("testKind", "testName");
    Entity shard0 = mdImpl.createMindashEntityShard(mindashKey, 0);
    shard0.setProperty(MindashDatastoreService.MindashShardCountLabel, 2100);
    shard0.setProperty("0", 0);
    shard0.setProperty("3000", 3000);
    List<Key> keys = new ArrayList<Key>(2100);
    keys.add(shard0.getKey());
    Map<Key, Entity> first1000Shards = new HashMap<Key, Entity>(1000);
    first1000Shards.put(shard0.getKey(), shard0);
    Map<Key, Entity> middle1000Shards = new HashMap<Key, Entity>(1000);
    Map<Key, Entity> last100Shards = new HashMap<Key, Entity>(100);
    for (int i = 1; i < 999; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(3000 + i), 3000 + i);
      keys.add(shard.getKey());
      first1000Shards.put(shard.getKey(), shard);
    }
    Entity shard999 = mdImpl.createMindashEntityShard(mindashKey, 999);
    shard999.setProperty("MyBlob", new Blob("blob start".getBytes()));
    keys.add(shard999.getKey());
    first1000Shards.put(shard999.getKey(), shard999);
    for (int i = 1000; i < 2000; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty("MyBlob", new Blob(String.valueOf(i).getBytes()));
      keys.add(shard.getKey());
      middle1000Shards.put(shard.getKey(), shard);
    }
    Entity shard2000 = mdImpl.createMindashEntityShard(mindashKey, 2000);
    shard2000.setProperty("MyBlob", new Blob("blob end".getBytes()));
    keys.add(shard2000.getKey());
    last100Shards.put(shard2000.getKey(), shard2000);
    for (int i = 2001; i < 2100; i++) {
      Entity shard = mdImpl.createMindashEntityShard(mindashKey, i);
      shard.setProperty(String.valueOf(i), i);
      shard.setProperty(String.valueOf(3000 + i), 3000 + i);
      keys.add(shard.getKey());
      last100Shards.put(shard.getKey(), shard);
    }
    expect(datastore.get(shard0.getKey())).andReturn(shard0);
    expect(datastore.get(keys.subList(0, 1000))).andReturn(first1000Shards);
    expect(datastore.get(keys.subList(1000, 2000))).andReturn(middle1000Shards);
    expect(datastore.get(keys.subList(2000, 2100))).andReturn(last100Shards);
    replay(datastore);
    Entity result = md.get(mindashKey);
    String expected = new String("blob start");
    for (int i = 1000; i < 2000; i++) {
      expected =
          expected
              + new String(new Blob(String.valueOf(i).getBytes()).getBytes());
    }
    expected = expected + "blob end";
    String resultProperty =
        new String(((Blob) result.getProperty("MyBlob")).getBytes());
    assertTrue("Result should have one property 'MyBlob' with correct value",
        expected.equals(resultProperty));
    assertTrue("Result should have property '2001' (immediately after blob)"
        + " with value '2001'", (Integer) result.getProperty("2001") == 2001);
  }

  @Test
  public void concatenateBlobShouldConcatenateCorrectly() {
    Blob head = new Blob("head".getBytes());
    Blob tail = new Blob("tail".getBytes());
    Blob result = mdImpl.concatenateBlob(head, tail);
    assertTrue("Concatenation of 'head' and 'tail' should be 'headtail'",
        "headtail".equals(new String(result.getBytes())));
  }

  // @Test
  // public void testMindashDatastoreServiceImplGetTransactionKey(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplGetTransactionIterableKey(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplGetIterableKey(){
  // assertTrue("Not implemented", false);
  // }

  @Test
  public void getActiveTransactionsShouldCallDatastoreGetActiveTransactions() {
    expect(datastore.getActiveTransactions()).andReturn(
        new ArrayList<Transaction>());
    replay(datastore);
    Collection<Transaction> ts = md.getActiveTransactions();
    verify(datastore);
    assertTrue(ts.isEmpty());
  }

  @Test
  public void getCurrentTransactionShouldCallDatastoreGetCurrentTransaction() {
    expect(datastore.getCurrentTransaction()).andReturn(transaction);
    replay(datastore);
    Transaction t = md.getCurrentTransaction();
    verify(datastore);
    assertTrue(t.equals(transaction));
  }

  @Test
  public void getCurrentTransactionTransactionShouldCallDatastoreGetCurrentTransactionTransaction() {
    expect(datastore.getCurrentTransaction(transaction)).andReturn(transaction);
    replay(datastore);
    Transaction t = md.getCurrentTransaction(transaction);
    verify(datastore);
    assertTrue(t.equals(transaction));
  }

  // @Test
  // public void testMindashDatastoreServiceImplPrepareQuery(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplPrepareTransactionQuery(){
  // assertTrue("Not implemented", false);
  // }

  // @Test
  // public void putEntityNoKeyShouldWork(){
  // Entity e = new Entity("testKind");
  // md.put(e);
  // e.setProperty(MindashDatastoreService.MindashShardCountLabel, 1);
  // verify(datastore).put(
  // }

  @Test
  public void putEntityNoKeyShouldPreemptivelySaveEntityToCompleteKey() {
    Entity entity = new Entity("testKind");
    expect(
        datastore
            .put(EntityWithIncompleteKeyEquals.eqEntityWithIncompleteKey()))
        .andReturn(KeyFactory.createKey("testKind", 1));
    replay(datastore);
    md.put(entity);
    verify(datastore);
  }

  @Test
  public void putEntityShouldStoreShardsByAddingIntermediateLayer() {
    Entity entity = new Entity("testKind");
    expect(
        datastore
            .put(EntityWithIncompleteKeyEquals.eqEntityWithIncompleteKey()))
        .andReturn(KeyFactory.createKey("testKind", 1));
    expect(
        datastore.put(ListOfEntitiesWithMddLayerEquals
            .eqListOfEntitiesWithMddLayer("testKind"))).andReturn(
        new ArrayList<Key>()).once();
    replay(datastore);
    md.put(entity);
    verify(datastore);
  }

  @Test
  public void putEntityShouldSplitStoring510ShardsIntoBatchOf500AndBatchOf10() {
    Entity entity = new Entity("testKind", "testName");
    /* generate blob to insure 1 property per entity */
    byte[] buffer = generateByteBuffer(900000);
    Blob blob = new Blob(buffer);
    for (int i = 0; i < 510; i++) {
      entity.setProperty(String.valueOf(i), blob);
    }
    /* this is about 510MB entity!!!! */
    expect(datastore.put(ListOfEntitiesSizeEquals.eqListOfEntitiesSize(500)))
        .andReturn(new ArrayList<Key>()).once();
    expect(datastore.put(ListOfEntitiesSizeEquals.eqListOfEntitiesSize(10)))
        .andReturn(new ArrayList<Key>()).once();
    replay(datastore);
    md.put(entity);
    verify(datastore);
  }

  // @Test
  // public void
  // putEntitesIterableShouldPreemptivelySaveEntitiesToCompleteIncompleteKeys(){
  // Entity e1 = new Entity("testKind");
  // Entity e2 = new Entity("testKind2");
  // List<Entity> entities = new ArrayList<Entity>(2);
  // entities.add(e1);
  // entities.add(e2);
  // List<Key> keys = md.put((Iterable<Entity>) entities);
  // verify(datastore).put(argThat(new IsListOfIncompleteKeyEntities()));
  // }

  // @Test
  // public void testMindashDatastoreServiceImplPutTransactionEntity(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplPutTransactionIterableEntity(){
  // assertTrue("Not implemented", false);
  // }
  //  
  // @Test
  // public void testMindashDatastoreServiceImplPutIterableEntity(){
  // assertTrue("Not implemented", false);
  // }
}
