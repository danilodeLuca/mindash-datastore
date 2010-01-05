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

import java.util.ArrayList;
import java.util.List;

import org.easymock.IArgumentMatcher;

import com.google.appengine.api.datastore.Entity;
import com.mindash.datastore.MindashDatastoreService;

/**
 * @author Tristan Slominski
 * 
 */
public class ListOfEntitiesWithMddLayerEquals implements IArgumentMatcher {

  private String testKind;

  public ListOfEntitiesWithMddLayerEquals(String testKind) {
    this.testKind = testKind;
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqListOfEntitiesWithMddLayer(kind: " + testKind + ")");
  }

  public static ArrayList<Entity> eqListOfEntitiesWithMddLayer(String testKind) {
    reportMatcher(new ListOfEntitiesWithMddLayerEquals(testKind));
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean matches(Object list) {
    if (!ArrayList.class.isInstance(list)) {
      return false;
    }
    List<Entity> l = (List<Entity>) list;
    for (Entity e : l) {
      if (e.getKey() == null
          || e.getKey().getParent() == null
          || e.getKey().getParent().getKind() == null
          || !e.getKey().getParent().getKind().equals(testKind)
          || !e.getKey().getKind().equals(testKind)
          || e.getKey().getName() == null
          || !e.getKey().getName().matches(
              MindashDatastoreService.MindashNamePrefixLabel + ".*")) {
        return false;
      }
    }
    return true;
  }

}
