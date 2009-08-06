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

import java.util.HashMap;
import java.util.Map;

import com.google.apphosting.api.ApiProxy;

/**
 * Test environment for JUnit testing.<br/>
 * Source: http://code.google.com/appengine/docs/java/howto/unittesting.html
 * @author Tristan Slominski
 *
 */
public class TestEnvironment implements ApiProxy.Environment {

  public String getAppId() {
    return "Unit Tests";
  }

  public Map<String, Object> getAttributes() {
    return new HashMap<String, Object>();
  }

  public String getAuthDomain() {
    return "gmail.com";
  }

  public String getEmail() {
    return "";
  }

  public String getRequestNamespace() {
    return "gmail.com";
  }

  public String getVersionId() {
    return "1.0";
  }

  public boolean isAdmin() {
    return false;
  }

  public boolean isLoggedIn() {
    return false;
  }

}
