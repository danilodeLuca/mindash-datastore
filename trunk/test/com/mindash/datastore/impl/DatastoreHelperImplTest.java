/**
 * Copyright 2010 Tristan Slominski
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.
 */
package com.mindash.datastore.impl;

import static org.easymock.classextension.EasyMock.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import atunit.AtUnit;
import atunit.Container;
import atunit.MockFramework;
import atunit.Unit;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.mindash.datastore.DatastoreHelper;
import com.mindash.datastore.LocalDatastoreTestCase;
import com.mindash.datastore.impl.DatastoreHelperImpl;

/**
 * @author Tristan Slominski
 *
 */
@RunWith(AtUnit.class)
@MockFramework(MockFramework.Option.EASYMOCK)
@Container(Container.Option.GUICE)
public class DatastoreHelperImplTest extends LocalDatastoreTestCase 
    implements Module{
  
  @Inject @Unit DatastoreHelper helper;
  
  @Inject DatastoreService datastore;
  
  @Override
  public void configure(Binder b) {
    b.bind(DatastoreService.class).toInstance(createMock(DatastoreService.class));
    b.bind(DatastoreHelper.class).to(DatastoreHelperImpl.class);
  }
  
  @Test
  public void getShouldRetrieveLessThan1000EntitiesInOneTryOnDatastore(){
    List<Key> keys = new ArrayList<Key>(900);
    for (int i = 0; i < 900; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    expect(datastore.get(keys)).andReturn(null).once();
    replay(datastore);
    helper.get(datastore, keys);
    verify(datastore);
  }
  
  @Test
  public void getShouldRetrieve1500EntitiesInTwoTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(1500);
    for (int i = 0; i < 1500; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    expect(datastore.get(keys.subList(0, 1000))).andReturn(null).once();
    expect(datastore.get(keys.subList(1000, 1500))).andReturn(null).once();
    replay(datastore);
    helper.get(datastore, keys);
    verify(datastore);
  }
  
  @Test
  public void getShouldRetrieve2200EntitiesInThreeTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(2200);
    for (int i = 0; i < 2200; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    expect(datastore.get(keys.subList(0, 1000))).andReturn(null).once();
    expect(datastore.get(keys.subList(1000, 2000))).andReturn(null).once();
    expect(datastore.get(keys.subList(2000, 2200))).andReturn(null).once();
    replay(datastore);
    helper.get(datastore, keys);
    verify(datastore);
  }
  
  @Test
  public void putShouldPutLessThan500EntitiesInOneTryOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(400);
    for (int i = 0; i < 400; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    expect(datastore.put(entities)).andReturn(null).once();
    replay(datastore);
    helper.put(datastore, entities);
    verify(datastore);
  }
  
  @Test
  public void putShouldPut900EntitiesInTwoTriesOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(900);
    for (int i = 0; i < 900; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    expect(datastore.put(entities.subList(0, 500))).andReturn(null).once();
    expect(datastore.put(entities.subList(500, 900))).andReturn(null).once();
    replay(datastore);
    helper.put(datastore, entities);
    verify(datastore);
  }
  
  @Test
  public void putShouldPut1200EntitiesInThreeTriesOnDatastore(){
    List<Entity> entities = new ArrayList<Entity>(1200);
    for(int i = 0; i < 1200; i++){
      Entity e = new Entity("testKind", "a" + String.valueOf(i));
      entities.add(e);
    }
    expect(datastore.put(entities.subList(0, 500))).andReturn(null).once();
    expect(datastore.put(entities.subList(500, 1000))).andReturn(null).once();
    expect(datastore.put(entities.subList(1000, 1200))).andReturn(null).once();
    replay(datastore);
    helper.put(datastore, entities);
    verify(datastore);
  }
  
  @Test
  public void deleteShouldDeleteLessThan500EntitiesInOneTryOnDatastore(){
    List<Key> keys = new ArrayList<Key>(400);
    for (int i = 0; i < 400; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    datastore.delete(keys);
    expectLastCall().once();
    replay(datastore);
    helper.delete(datastore, keys);
    verify(datastore);
  }
  
  @Test
  public void deleteShouldDelete900EntitiesInTwoTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(900);
    for (int i = 0; i<900; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    datastore.delete(keys.subList(0, 500));
    expectLastCall().once();
    datastore.delete(keys.subList(500, 900));
    expectLastCall().once();
    replay(datastore);
    helper.delete(datastore, keys);
    verify(datastore);
  }
  
  @Test
  public void deleteShouldDelete1200EntitiesInThreeTriesOnDatastore(){
    List<Key> keys = new ArrayList<Key>(1200);
    for (int i = 0; i < 1200; i++){
      Key k = KeyFactory.createKey("testKind", "a" + String.valueOf(i));
      keys.add(k);
    }
    datastore.delete(keys.subList(0, 500));
    expectLastCall().once();
    datastore.delete(keys.subList(500, 1000));
    expectLastCall().once();
    datastore.delete(keys.subList(1000, 1200));
    expectLastCall().once();
    replay(datastore);
    helper.delete(datastore, keys);
    verify(datastore);
  }

}
