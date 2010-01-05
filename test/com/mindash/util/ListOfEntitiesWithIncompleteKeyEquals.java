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
package com.mindash.util;

import static org.easymock.EasyMock.reportMatcher;

import java.util.List;

import org.easymock.IArgumentMatcher;

import com.google.appengine.api.datastore.Entity;

/**
 * @author Tristan Slominski
 *
 */
public class ListOfEntitiesWithIncompleteKeyEquals implements IArgumentMatcher {

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqListOfEntitiesWithIncompleteKey()");
  }

  public static List<Entity> eqListOfEntitiesWithIncompleteKey(){
    reportMatcher(new ListOfEntitiesWithIncompleteKeyEquals());
    return null;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean matches(Object list) {
    if (Iterable.class.isInstance(list)) {
      List<Entity> l = (List<Entity>) list;
      for (Entity e : l) {
        if (e.getKey().isComplete()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

}
