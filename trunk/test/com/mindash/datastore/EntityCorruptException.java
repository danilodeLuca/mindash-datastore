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

/**
 * Thrown when all of the shards of an entity are not available from
 * the datastore. Partial data may be retrieved, but the entity integrity
 * is lost.
 * 
 * @author Tristan Slominski
 */
@SuppressWarnings("serial")
public class EntityCorruptException extends Exception {
  
  public EntityCorruptException(){
    super();
  }
  
  public EntityCorruptException(String message){
    super(message);
  }
  
  public EntityCorruptException(Throwable cause){
    super(cause);
  }
  
  public EntityCorruptException(String message, Throwable cause){
    super(message, cause);
  }

}
