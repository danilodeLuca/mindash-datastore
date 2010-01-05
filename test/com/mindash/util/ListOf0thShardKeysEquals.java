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


import static org.easymock.classextension.EasyMock.*;
import java.util.List;

import org.easymock.IArgumentMatcher;

import com.google.appengine.api.datastore.Key;
import com.mindash.datastore.MindashDatastoreService;

/**
 * @author Tristan Slominski
 *
 */
public class ListOf0thShardKeysEquals implements IArgumentMatcher {

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eq0thShardKeys()");
  }

  public static List<Key> eqListOf0thShardKeys(){
    reportMatcher(new ListOf0thShardKeysEquals());
    return null;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean matches(Object list) {
    if (List.class.isInstance(list)) {
      List<Key> l = (List<Key>) list;
      for (Key k : l) {
        if (!k.getName().equals(
            MindashDatastoreService.MindashNamePrefixLabel + 0)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

}
