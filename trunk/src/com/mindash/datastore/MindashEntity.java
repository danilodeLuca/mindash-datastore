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

import java.util.Map;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

/**
 * <p>Mindash equivalent of Google's <code>Entity</code> object.</p>
 * <p>Since Google made <code>Entity final</code>, <code>MindashEntity</code>
 * cannot extend and must be implemented as a separate object.</p>
 * 
 * @author Tristan Slominski
 *
 */
public class MindashEntity {
  
  private Entity entity;

  // TODO[Tristan]: implement
  public MindashEntity(String kind){
    entity = new Entity(kind);
  }
  
  // TODO[Tristan]: implement
  public MindashEntity(String kind, Key parent){
    entity = new Entity(kind, parent);
  }
  
  // TODO[Tristan]: implement
  public MindashEntity(String kind, String name){
    entity = new Entity(kind, name);
  }
  
  // TODO[Tristan]: implement
  public MindashEntity(String kind, String name, Key parent){
    entity = new Entity(kind, name, parent); 
  }
  
  /**
   * This method just passes the <code>getAppId()</code> call
   * to the underlying <code>Entity</code>.
   * @return the identifier of the application that owns the underlying 
   * <code>Entity</code>. This is simply a convenience method that forwards 
   * to the <code>Key</code> for the underlying <code>Entity</code>. 
   */
  public String getAppId(){
    // TODO[Tristan]: change this when implemented MindashEntity as a collection
    //                of multiple Google Entities.
    return entity.getAppId();
  }
  
  // TODO[Tristan]: implement
  public Key getKey(){
    return null;
  }
  
  // TODO[Tristan]: implement
  public String getKind(){
    return null;
  }
  
  // TODO[Tristan]: implement
  public String getNamespace(){
    return null;
  }
  
  // TODO[Tristan]: implement
  public Key getParent(){
    return null;
  }
  
  // TODO[Tristan]: implement
  public Map<String,Object> getProperties(){
    return null;
  }
  
  // TODO[Tristan]: implement
  public Object getProperty(String propertyName){
    return null;
  }
  
  // TODO[Tristan]: implement
  public boolean hasProperty(String propertyName){
    return false;
  }
  
  // TODO[Tristan]: implement
  public void removeProperty(String propertyName){
    
  }
  
  // TODO[Tristan]: implement
  public void setPropertiesFrom(MindashEntity mEntity){
    
  }
  
  // TODO[Tristan]: implement
  public void setProperty(String propertyName, Object value){
    
  }
  
  // TODO[Tristan]: implement
  public void setUnindexedProperty(String propertyName, Object value){
    
  }
  
}
