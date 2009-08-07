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

import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.mindash.util.TestUtilities;

/**
 * Test for <code>MindashDatastoreServiceImpl</code>
 * @author Tristan Slominski
 *
 */
public class MindashDatastoreServiceImplTest extends LocalDatastoreTestCase{
  
  private MindashDatastoreService md;
  
  @Before
  public void setUp(){
    super.setUp();
    md = new MindashDatastoreServiceImpl();
  }

  /** TEST CONSTRUCTORS */
  
  /** TEST METHODS */
  
  @Test
  public void testMindashDatastoreServiceImplBeginTransaction(){
    Transaction txn = md.beginTransaction();
    assertTrue("Should start a new transaction", txn != null);
    assertTrue("Started transaction should be active", txn.isActive());
  }
  
  @Test
  public void testMindashDatastoreServiceImplDeleteKey(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplDeleteTransactionKeys(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplDeleteTransactionIterableKey(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServideImplDeleteIterableKey(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetKey(){
    // retrieving an entity should work
    Entity e = new Entity("test");
    Transaction txn = md.beginTransaction();
    Key key = md.put(e);
    txn.commit();
    e = null;
    try {
      txn = md.beginTransaction();
      e = md.get(key);
      txn.commit();
    } catch (EntityNotFoundException e1) {
      fail("Should be able to retrieve the entity after saving it");
    }
    if ( e.equals(e) ){
      assertTrue(true);
    } else {
      fail("Retrieved entity should be the same as the saved one");
    }
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetTransactionKey(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetTransactionIterableKey(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetIterableKey(){
    assertTrue("Not implemented", false);
  }
  
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
  
  @Test
  public void testMindashDatastoreServiceImplPrepareQuery(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplPrepareTransactionQuery(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplPutEntity(){
    DatastoreService datastore = 
        (DatastoreService) TestUtilities.getPrivateField(md, "datastore");
    
    // save an entity with key as long id (unspecified)
    Entity e = new Entity("test");
    Transaction txn = md.beginTransaction();
    Key key = md.put(e);
    txn.commit();
    // an extra "mdd" layer should be added to the entity
    try {
      txn = datastore.beginTransaction();
      datastore.get(key);
      txn.commit();
      fail("An extra \"mdd\" layer should have been added to the entity (key " +
          "long id (unspecified); i.e. the key should be [test,id][mdd,id], " +
          "not [test,id]");
    } catch (EntityNotFoundException e1) {
      assertTrue(true);
    }
    
    // save an entity with key as string name (specified)
    e = new Entity("test","test");
    txn = md.beginTransaction();
    key = md.put(e);
    txn.commit();
    // an extra "mdd" layer should be added to the entity
    try {
      txn = datastore.beginTransaction();
      datastore.get(key);
      txn.commit();
      fail("An extra \"mdd\" layer should have been added to the entity (key " +
          "string name (specified); i.e. the key should be [test,id][mdd,id]," +
          " not [test,id]");
    } catch (EntityNotFoundException e1) {
      assertTrue(true);
    }
    
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
  
  @Test
  public void testMindashDatastoreServiceImplPutTransactionEntity(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplPutTransactionIterableEntity(){
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplPutIterableEntity(){
    assertTrue("Not implemented", false);
  }
}