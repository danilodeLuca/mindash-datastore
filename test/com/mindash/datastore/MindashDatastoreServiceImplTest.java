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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

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
import com.mindash.util.TestUtilities;

/**
 * Test for <code>MindashDatastoreServiceImpl</code>
 * @author Tristan Slominski
 *
 */
public class MindashDatastoreServiceImplTest extends LocalDatastoreTestCase{
  
  private MindashDatastoreService md;
  private DatastoreService datastore;
  
  @Before
  public void setUp(){
    super.setUp();
    md = new MindashDatastoreServiceImpl();
    datastore = (DatastoreService) 
        TestUtilities.getPrivateField(md, "datastore");
  }

  /** TEST CONSTRUCTORS */
  
  /** TEST METHODS */
  
  @Test
  public void testMindashDatastoreServiceImplBeginTransaction(){
    Transaction txn = md.beginTransaction();
    assertTrue("Should start a new transaction", txn != null);
    assertTrue("Started transaction should be active", txn.isActive());
  }
  
//  @Test
//  public void testMindashDatastoreServiceImplDeleteKey(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplDeleteTransactionKeys(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplDeleteTransactionIterableKey(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServideImplDeleteIterableKey(){
//    assertTrue("Not implemented", false);
//  }
  
  @Test
  public void testMindashDatastoreServiceImplGetKey(){
    // retrieving an entity should work
    Entity e = new Entity("test");
    Key key = executeMdPut(e);
    e = null;
    try {
      Transaction txn = md.beginTransaction();
      e = md.get(key);
      txn.commit();
    } catch (EntityNotFoundException e1) {
      fail("Should be able to retrieve the entity after saving it");
    } catch (EntityCorruptException ex) {
      fail("Entity is corrupt");
    }
    if ( e.equals(e) ){
      assertTrue(true);
    } else {
      fail("Retrieved entity should be the same as the saved one");
    }
  }
  
//  @Test
//  public void testMindashDatastoreServiceImplGetTransactionKey(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplGetTransactionIterableKey(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplGetIterableKey(){
//    assertTrue("Not implemented", false);
//  }
  
  @Test
  public void testMindashDatastoreServiceImplGetActiveTransactions(){
    assertTrue("There should be only one active transaction to start with",
        md.getActiveTransactions().size() == 1);
    
    Transaction txn = md.beginTransaction();
    assertTrue("There should be two active transactions after starting one",
        md.getActiveTransactions().size() == 2);
    
    txn.commit();
    assertTrue("There should be only one active transaction after committing" +
        " one of the two transactions", md.getActiveTransactions().size() == 1);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetCurrentTransaction(){
    Transaction originalTxn = md.getCurrentTransaction();
    assertTrue("There should exist a default transaction",
        originalTxn != null);
    
    Transaction txn = md.beginTransaction();
    assertTrue("Current transaction should be the most recent one",
        md.getCurrentTransaction() == txn);
    
    txn.commit();
    assertTrue("Current transaction should be the original after others are " +
        "committed", md.getCurrentTransaction() == originalTxn);
    
    txn = md.beginTransaction();
    txn.rollback();
    assertTrue("Current transaction should be the orginal after others are " +
        "crolled back", md.getCurrentTransaction() == originalTxn);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetCurrentTransactionTransaction(){
    Transaction originalTxn = md.getCurrentTransaction();
    Transaction originalTxn2 = md.getCurrentTransaction(null);
    assertTrue("Current transaction should be returned", 
        originalTxn2 == originalTxn);
    
    Transaction txn = md.beginTransaction();
    assertTrue("Current transaction should be the most recent one",
        md.getCurrentTransaction(null) == txn);
        
    txn.commit();
    assertTrue("Current transaction should be the original after others are " +
        "committed", md.getCurrentTransaction(null) == originalTxn);
        
    txn = md.beginTransaction();
    txn.rollback();
    assertTrue("Current transaction should be the orginal after others are " +
        "crolled back", md.getCurrentTransaction(null) == originalTxn);
  }
  
//  @Test
//  public void testMindashDatastoreServiceImplPrepareQuery(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplPrepareTransactionQuery(){
//    assertTrue("Not implemented", false);
//  }
  
  @Test
  public void PutEntityNoKey(){
    // save an entity with key as long id (unspecified)
    Entity e = new Entity("test");
    Key key = executeMdPut(e);
    // an extra "mdd" layer should be added to the entity
    try {
      Transaction txn = datastore.beginTransaction();
      datastore.get(key);
      txn.commit();
      fail("An extra \"mdd\" layer should have been added to the entity (key " +
          "long id (unspecified); i.e. the key should be [test,id][mdd,id], " +
          "not [test,id]");
    } catch (EntityNotFoundException e1) {
      assertTrue(true);
    }
  }
  
  @Test
  public void PutEntityStringKey(){
    // save an entity with key as string name (specified)
    Entity e = new Entity("test","test");
    Key key = executeMdPut(e);
    // an extra "mdd" layer should be added to the entity
    try {
      Transaction txn = datastore.beginTransaction();
      datastore.get(key);
      txn.commit();
      fail("An extra \"mdd\" layer should have been added to the entity (key " +
          "string name (specified); i.e. the key should be [test,id][mdd,id]," +
          " not [test,id]");
    } catch (EntityNotFoundException e1) {
      assertTrue(true);
    }
  }
  
  @Test
  public void PutEntityOneStringProperty(){
    // save an entity with small property, should be only one shard
    Entity e = new Entity("testProperty1","test");
    e.setProperty("property1", "This is a string property");
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be 'This is a string property'",
        ((String)e.getProperty("property1"))
            .equals("This is a string property"));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneIntegerProperty(){
    Entity e = new Entity("testProperty1", "test");
    e.setProperty("property1", 1);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be " + 1, 
        ((Long)e.getProperty("property1")) == 1);
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneDoubleProperty(){
    Entity e = new Entity("testProperty1", "test");
    e.setProperty("property1", (double) 1.0);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be " + 1.0,
        ((Double)e.getProperty("property1")) == 1.0);
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneKeyProperty(){
    Entity e = new Entity("testProperty1", "test");
    Key myKey = KeyFactory.createKey("myKind", 1732);
    e.setProperty("property1", myKey);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be " + KeyFactory.keyToString(myKey),
        ((Key)e.getProperty("property1")).equals(myKey));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneUserProperty(){
    Entity e = new Entity("testProperty1", "test");
    User user = new User("example@email.com", "google.com");
    e.setProperty("property1", user);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be User with authDomain: " + 
        user.getAuthDomain() + " email: " + user.getEmail() + " nickname " +
        user.getNickname(),((User)e.getProperty("property1")).equals(user));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneShortBlobProperty(){
    Entity e = new Entity("testProperty1", "test");
    ShortBlob sb = new ShortBlob("This is some short blob".getBytes());
    e.setProperty("property1", sb);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be: " + new String(sb.getBytes()),
        ((ShortBlob)e.getProperty("property1")).equals(sb));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneDateProperty(){
    Entity e = new Entity("testProperty1", "test");
    Date date = new Date();
    e.setProperty("property1", date);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be: " + date.getTime(),
        ((Date) e.getProperty("property1")).equals(date));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneLinkProperty(){
    Entity e = new Entity("testProperty1", "test");
    Link link = new Link("this is some link");
    e.setProperty("property1", link);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be: " + link.getValue(),
        ((Link) e.getProperty("property1")).equals(link));
    
    // should only be 0 shard
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneBlobPropertyShortLength(){
    Entity e = new Entity("testProperty1", "test");
    Blob blob = new Blob("this is a normal sized blob".getBytes());
    e.setProperty("property1", blob);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' shoudl be: " + new String(blob.getBytes()),
        ((Blob)e.getProperty("property1")).equals(blob));
    
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneBlobPropertyLongLength(){
    Entity e = new Entity("testProperty1", "test");
    // generate really long blob
    String fileName = "semantics.pdf";
    String path = "test/com/mindash/datastore/" + fileName;
    File book = new File(path);
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(book);
    } catch (FileNotFoundException e1){
      fail("File \"" + fileName + "\" not found at \"" + path + "\"");
    }
    byte[] buffer = new byte[2000000];
    try {
      fis.read(buffer);
    } catch (IOException e1) {
      fail("IOException!");
    }
    Blob blob = new Blob(buffer);
    e.setProperty("property1", blob);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("shard 0 should not have the entire long blob",
        !((Blob)e.getProperty("property1")).equals(blob));
    
    // shard 1 should exist
    getEntityFromGoogleDatastore(key, 1);
    
    CheckForNoExtraShards(key, 2);
  }
  
  @Test
  public void PutEntityOneTextPropertyShortLength(){
    Entity e = new Entity("testProperty1", "test");
    Text text = new Text("this is normal sized text");
    e.setProperty("property1", text);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("'property1' should be: " + text.getValue(),
        ((Text)e.getProperty("property1")).equals(text));
    
    CheckForNoExtraShards(key, 1);
  }
  
  @Test
  public void PutEntityOneTextPropertyLongLength(){
    Entity e = new Entity("testProperty1", "test");
    // generate really long text
    String fileName = "semantics.pdf";
    String path = "test/com/mindash/datastore/" + fileName;
    File book = new File(path);
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(book);
    } catch (FileNotFoundException e1){
      fail("File \"" + fileName + "\" not found at \"" + path + "\"");
    }
    byte[] buffer = new byte[2000000];
    try {
      fis.read(buffer);
    } catch (IOException e1){
      fail("IOException!");
    }
    Text text = new Text(new String(buffer));
    e.setProperty("property1", text);
    Key key = executeMdPut(e);
    
    CheckForExtraMDDLayer(key);
    CheckFor0thShard(key);
    
    e = getEntityFromGoogleDatastore(key, 0);
    
    assertTrue("shard 0 should not have the entire long text",
        !((Text)e.getProperty("property1")).equals(text));
    
    // shard 1 should exist
    getEntityFromGoogleDatastore(key, 1);
    
    CheckForNoExtraShards(key, 2);
  }

  /**
   * @param e
   */
  private Key executeMdPut(Entity e) {
    Transaction txn = md.beginTransaction();
    Key key = md.put(e);
    txn.commit();
    return key;
  }
  
  private void CheckFor0thShard(Key key){
    // there should be [test,'test'][mdd,0] shard
    Key mdKey = KeyFactory.createKey(
            key, 
            MindashDatastoreService.MindashKindLayerLabel,
            MindashDatastoreService.MindashNamePrefixLabel + 0);
    try {
      datastore.get(mdKey);
    } catch (EntityNotFoundException e1) {
      fail("An entity [test,'test'][mdd,0] should exist");
    }
  }
  
  /**
   * Utility method to pull entities from original Google datastore
   * @param key the key
   * @param shard the shard to pull
   * @return the Entity
   */
  private Entity getEntityFromGoogleDatastore(Key key, int shard){
    Key mdKey = MindashDatastoreServiceImpl.createMindashDatastoreKey(key, 0);
    try {
      return datastore.get(mdKey);
    } catch (EntityNotFoundException e1) {
      fail("Entity should exist");
    }
    return null;
  }
  
  private void CheckForNoExtraShards(Key key, int extraShard){
    // there should be no other shards
    Key mdKey = KeyFactory.createKey(
            key,
            MindashDatastoreService.MindashKindLayerLabel,
            MindashDatastoreService.MindashNamePrefixLabel + extraShard);
    try {
      datastore.get(mdKey);
      fail("An entity [testProperty1,'test'][mdd," + extraShard + "] should " +
      		"not exist");
    } catch (EntityNotFoundException ex){
      assertTrue(true);
    }
  }
  
  private void CheckForExtraMDDLayer(Key key){
 // an extra "mdd"layer should be added to the entity
    try {
      Transaction txn = datastore.beginTransaction();
      datastore.get(key);
      txn.commit();
      fail("An extra \"mdd\" layer should have been added to the entity; i.e." +
          "the key should be [test,'test'][mdd,0], not [test,'test']");
    } catch (EntityNotFoundException ex) {
      assertTrue(true);
    }
  }
  
  @Test
  public void testMindashDatastoreServiceImplPutEntity(){

    

    
    
    // TODO: continue adding more tests
    
//    Entity e = new Entity("test");
//    Transaction txn = md.beginTransaction();
//    Key key = md.put(e);
//    txn.commit();
//    assertTrue("Should be able to store the default entity", key != null);
//    
//    e = new Entity("test");
//    e.setProperty("someProperty", "myValue");
//    txn = md.beginTransaction();
//    key = md.put(e);
//    txn.commit();
//    assertTrue("Should be able to store an entity with some properties set",
//        key != null);
//    
//    e = new Entity("test");
//    String fileName = "semantics.pdf";
//    String path = "test/com/mindash/datastore/" + fileName;
//    File book = new File(path);
//    FileInputStream fis = null;
//    try {
//      fis = new FileInputStream(book);
//    } catch (FileNotFoundException e1) {
//      fail("File \"" + fileName + "\" not found at \"" + path + "\"");
//    }
//    byte[] buffer = new byte[700000];
//    int iterations = 3;
//    while (iterations > 0){
//      try {
//        fis.read(buffer);
//        e.setProperty("block" + iterations, new Text(new String(buffer)));
//        iterations--;
//      } catch (IOException e1) {
//        fail("IOException!");
//      }
//    }
//    txn = md.beginTransaction();
//    int totalSize = 0;
//    Iterator<Entry<String,Object>> i = e.getProperties().entrySet().iterator();
//    while (i.hasNext()){
//      
//    }
//    key = md.put(e);
//    txn.commit();
//    assertTrue("Should be able to store an entity greater than 1MB",
//        key != null);
  }
  
//  @Test
//  public void testMindashDatastoreServiceImplPutTransactionEntity(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplPutTransactionIterableEntity(){
//    assertTrue("Not implemented", false);
//  }
//  
//  @Test
//  public void testMindashDatastoreServiceImplPutIterableEntity(){
//    assertTrue("Not implemented", false);
//  }
}
