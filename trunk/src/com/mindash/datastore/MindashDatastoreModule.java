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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.inject.AbstractModule;
import com.mindash.datastore.impl.DatastoreHelperImpl;
import com.mindash.datastore.impl.MindashDatastoreServiceImpl;
import com.mindash.datastore.impl.MindashPreparedQueryImpl;

/**
 * Google Guice Mindash Datastore Module for dependency injection.
 * 
 * @author Tristan Slominski
 */
public class MindashDatastoreModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(DatastoreHelper.class).to(DatastoreHelperImpl.class);
    bind(DatastoreService.class).toInstance(
        DatastoreServiceFactory.getDatastoreService());
    bind(MindashDatastoreService.class).to(MindashDatastoreServiceImpl.class);
    bind(MindashPreparedQuery.class).to(MindashPreparedQueryImpl.class);
  }

}
