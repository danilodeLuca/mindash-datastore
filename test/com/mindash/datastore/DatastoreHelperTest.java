/**
 * Copyright 2009 Tristan Slominski
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.
 */
package com.mindash.datastore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import atunit.AtUnit;
import atunit.Container;
import atunit.Unit;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.mindash.datastore.DatastoreHelperImpl;

/**
 * @author Tristan Slominski
 *
 */
@RunWith(AtUnit.class)
@Container(Container.Option.GUICE)
public class DatastoreHelperTest extends LocalDatastoreTestCase 
    implements Module{
  
  @Override
  public void configure(Binder b) {
    b.bind(DatastoreService.class).toInstance(mock(DatastoreService.class));
    b.bind(DatastoreHelper.class).to(DatastoreHelperImpl.class);
  }
  
  @Inject @Unit DatastoreHelper helper;
  
  @Inject DatastoreService datastore;
  
  @Test
  public void getShouldRetrieveLessThan1000EntitiesInOneTryOnDatastore(){
    List<Key> keys = new ArrayList<Key>(900);
    for (int i = 0; i < 900; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.get(datastore, keys);
    verify(datastore, times(1)).get(keys);
  }
  
  @Test
  public void getShouldRetrieve1500EntitiesInTwoTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(1500);
    for (int i = 0; i < 1500; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.get(datastore, keys);
    verify(datastore, times(1)).get(keys.subList(0, 1000));
    verify(datastore, times(1)).get(keys.subList(1000, 1500));
  }
  
  @Test
  public void getShouldRetrieve2200EntitiesInThreeTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(2200);
    for (int i = 0; i < 2200; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.get(datastore, keys);
    verify(datastore, times(1)).get(keys.subList(0, 1000));
    verify(datastore, times(1)).get(keys.subList(1000, 2000));
    verify(datastore, times(1)).get(keys.subList(2000, 2200));
  }
  
  @Test
  public void putShouldPutLessThan500EntitiesInOneTryOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(400);
    for (int i = 0; i < 400; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    helper.put(datastore, entities);
    verify(datastore, times(1)).put(entities);
  }
  
  @Test
  public void putShouldPut900EntitiesInTwoTriesOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(900);
    for (int i = 0; i < 900; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    helper.put(datastore, entities);
    verify(datastore, times(1)).put(entities.subList(0, 500));
    verify(datastore, times(1)).put(entities.subList(500, 900));
  }
  
  @Test
  public void putShouldPut1200EntitiesInThreeTriesOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(1200);
    for(int i = 0; i < 1200; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    helper.put(datastore, entities);
    verify(datastore, times(1)).put(entities.subList(0, 500));
    verify(datastore, times(1)).put(entities.subList(500, 1000));
    verify(datastore, times(1)).put(entities.subList(1000, 1200));
  }
  
  @Test
  public void deleteShouldDeleteLessThan500EntitiesInOneTryOnDatastore(){
    List<Key> keys = new ArrayList<Key>(400);
    for (int i = 0; i < 400; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.delete(datastore, keys);
    verify(datastore, times(1)).delete(keys);
  }
  
  @Test
  public void deleteShouldDelete900EntitiesInTwoTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(900);
    for (int i = 0; i<900; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.delete(datastore, keys);
    verify(datastore, times(1)).delete(keys.subList(0, 500));
    verify(datastore, times(1)).delete(keys.subList(500, 900));
  }
  
  @Test
  public void deleteShouldDelete1200EntitiesInThreeTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(1200);
    for (int i = 0; i < 1200; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    helper.delete(datastore, keys);
    verify(datastore, times(1)).delete(keys.subList(0, 500));
    verify(datastore, times(1)).delete(keys.subList(500, 1000));
    verify(datastore, times(1)).delete(keys.subList(1000, 1200));
  }

}
