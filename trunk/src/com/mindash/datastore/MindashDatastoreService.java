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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * The wrapper interface for Google's Datastore Service that enables storing
 * entities greater than 1MB.
 * 
 * @author Tristan Slominski
 */
public interface MindashDatastoreService {
  
  public static String MindashKindLayerLabel = "mdd";
  public static String MindashNamePrefixLabel = "mdd";
  public static String MindashShardCountLabel = "mddx";
  public static int MindashInitialEntityOverheadSize = 1024;
  public static int MindashEntityMaximumSize = 1024 * 1024;
  public static int MindashAssumedPropertyOverhead = 128;
  
	public Transaction beginTransaction();
	
	public void delete(Key... keys);
	
	public void delete(Transaction txn, Key... keys);
	
	public void delete(Transaction txn, Iterable<Key> keys);
	
	public void delete(Iterable<Key> keys);
	
	public Entity get(Key key) throws EntityNotFoundException,
	    EntityCorruptException;
	
	public Entity get(Transaction txn, Key key) throws EntityNotFoundException,
	    EntityCorruptException;
	
	public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) 
	    throws EntityCorruptException;
	
	public Map<Key, Entity> get(Iterable<Key> keys) throws EntityCorruptException;
	
	public Collection<Transaction> getActiveTransactions();
	
	public Transaction getCurrentTransaction();
	
	public Transaction getCurrentTransaction(Transaction txn);
	
	// TODO: implement
	public PreparedQuery prepare(Query query);
	
	// TODO: implement
	public PreparedQuery prepare(Transaction txn, Query query);
	
	public Key put(Entity entity);
	
	public Key put(Transaction txn, Entity entity);
	
	public List<Key> put(Transaction txn, Iterable<Entity> entities);
	
	public List<Key> put(Iterable<Entity> entities);
}
