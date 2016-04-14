This project is a framework for storing entities larger than 1MB using the Google App Engine low-level datastore API.

`MindashDatastoreService.java` is a plug and play replacement for `DatastoreService.java` provided by Google. It uses `DatastoreService` under the hood and abstracts all the maintenance related to storing large entities. If you can upload / create it in memory, you'll be able to store it in the Datastore. Additionally, Mindash Datastore implementation removes the 500 put / delete and 1000 get limits (see `DatastoreHelperImpl.java`).

Tests are included so you can see the extent to which the implementations have been tested.

Source code comments are enabled for everyone.

Alpha release 0.7 [com.mindash.datastore-0.7-src.jar](http://mindash-datastore.googlecode.com/files/com.mindash.datastore-0.7-src.jar) implements the `DatastoreService` interface with added `EntityCorruptException` (see below). Transactions, queries, and get(Iterable) & put(Iterable) have not yet been extensively tested, hence the Alpha release. The preparation of queries as `QueryResultIterable` has not yet been implemented.

The project is dependent on AppEngine SDK 1.3.0 and JUnit4. Other dependencies are included in the `lib` folder.

```
public interface MindashDatastoreService {
  
  public static String MindashNamePrefixLabel = "mdd";
  public static String MindashShardCountLabel = "mddx";
  public static int MindashInitialEntityOverheadSize = 1024;
  public static int MindashEntityMaximumSize = 1024 * 1024;
  public static int MindashAssumedPropertyOverhead = 128;
  
  public KeyRange allocateIds(Key parent, String kind, long num);
  
  public KeyRange allocateIds(String kind, long num);
  
  public Transaction beginTransaction();
	
  public void delete(Key... keys);
	
  public void delete(Transaction txn, Key... keys);
	
  public void delete(Transaction txn, Iterable<Key> keys);
	
  public void delete(Iterable<Key> keys);
	
  public Entity get(Key key) throws EntityNotFoundException, EntityCorruptException;
	
  public Entity get(Transaction txn, Key key) throws EntityNotFoundException, EntityCorruptException;
	
  public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) throws EntityCorruptException;
	
  public Map<Key, Entity> get(Iterable<Key> keys) throws EntityCorruptException;
	
  public Collection<Transaction> getActiveTransactions();
	
  public Transaction getCurrentTransaction();
	
  public Transaction getCurrentTransaction(Transaction txn);
	
  public MindashPreparedQuery prepare(Query query);
	
  public MindashPreparedQuery prepare(Transaction txn, Query query);
	
  public Key put(Entity entity);
	
  public Key put(Transaction txn, Entity entity);
	
  public List<Key> put(Transaction txn, Iterable<Entity> entities);
	
  public List<Key> put(Iterable<Entity> entities);

}
```
```
public interface MindashPreparedQuery {

  public Iterable<Entity> asIterable();

  public Iterable<Entity> asIterable(FetchOptions fetchOptions);

  public Iterator<Entity> asIterator();

  public Iterator<Entity> asIterator(FetchOptions fetchOptions);

  public List<Entity> asList(FetchOptions fetchOptions);

  public QueryResultIterable<Entity> asQueryResultIterable() throws NotImplementedException;

  public QueryResultIterable<Entity> asQueryResultIterable(FetchOptions fetchOptions) throws NotImplementedException;

  public QueryResultIterator<Entity> asQueryResultIterator() throws NotImplementedException;

  public QueryResultIterator<Entity> asQueryResultIterator(FetchOptions fetchOptions) throws NotImplementedException;

  public QueryResultList<Entity> asQueryResultList(FetchOptions fetchOptions) throws NotImplementedException;

  public Entity asSingleEntity() throws TooManyResultsException;

  public int countEntities();

}
```