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

import java.io.File;

import org.junit.After;
import org.junit.Before;

import com.google.appengine.tools.development.ApiProxyLocalImpl;
import com.google.apphosting.api.ApiProxy;

/**
 * Setup and teardown methods for local tests.<br/>
 * Source: http://code.google.com/appengine/docs/java/howto/unittesting.html
 * @author Tristan Slominski
 *
 */
public class LocalTestCase {
  
  @Before
  public void setUp(){
    ApiProxy.setEnvironmentForCurrentThread(new TestEnvironment());
    ApiProxy.setDelegate(new ApiProxyLocalImpl(new File(".")){});
  }

  @After
  public void tearDown(){
    ApiProxy.setDelegate(null);
    ApiProxy.setEnvironmentForCurrentThread(null);
  }
}
