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

import org.junit.Before;
import org.junit.Test;

import com.google.appengine.api.datastore.Transaction;

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
    assertTrue("Not implemented", false);
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
    assertTrue("Not implemented", false);
  }
  
  @Test
  public void testMindashDatastoreServiceImplGetCurrentTransactionTransaction(){
    assertTrue("Not implemented", false);
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
    assertTrue("Not implemented", false);
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
