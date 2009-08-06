/**
 * Copyright 2009 Tristan Slominski
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed herein is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.mindash.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.AssertionFailedError;

/**
 * Test utilities:
 * <ul>
 * <li>{@link #invokeMethod(Class, String, Class[], Object[])} - calls private
 * void method</li>
 * <li>{@link #invokeMethod(Class, Object, String, Class[], Object[])} - calls
 * private method and returns its value</li>
 * <li>{@link #setPrivateField(Object, String, Object)} - sets a private field
 * </li>
 * <li>{@link #getPrivateField(Object, String)} - gets a private field</li>
 * </ul>
 * @author Tristan Slominski
 */
public class TestUtilities {

  /**
   * A method for calling a private void method.<br/>
   * Source of the method: <a 
   * href="http://www.artima.com/suiterunner/private3.html">
   * http://www.artima.com/suiterunner/private3.html</a>
   * @param targetClass the target class containing the private method
   * @param methodName the name of the private method
   * @param argClasses the classes of parameters
   * @param argObjects the parameters themselves
   * @throws InvocationTargetException
   */
  @SuppressWarnings("unchecked")
  public static void invokeMethod(Class targetClass, String methodName,
      Class[] argClasses, Object[] argObjects) 
      throws InvocationTargetException {
    try {
      Method method = targetClass.getDeclaredMethod(methodName, argClasses);
      method.setAccessible(true);
      method.invoke(null, argObjects);
    } catch (NoSuchMethodException e){
      // Should happen only rarely, because most times the
      // specified method should exist. If it does happen, just let
      // the test fail so the programmer can fix the problem.
      throw new AssertionFailedError();
    } catch (SecurityException e) {
      // Should happen only rarely, because the setAccessible(true)
      // should be allowed in when running unit tests. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (IllegalAccessException e) {
      // Should never happen, because setting accessible flag to
      // true. If setting accessible fails, should throw a security
      // exception at that point and never get to the invoke. But
      // just in case, wrap it in a AssertionFailedError and let a
      // human figure it out.
      throw new AssertionFailedError();
    } catch (IllegalArgumentException e) {
      // Should happen only rarely, because usually the right
      // number and types of arguments will be passed. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    }
  }
  
  /**
   * A method for calling a private method which returns a value.
   * @param targetClass the class containing the private method
   * @param targetObject an instance of the object to call the method from
   * @param methodName the name of the method
   * @param argClasses the classes of parameters
   * @param argObjects the parameters themselves
   * @return the invoked method's return object
   * @throws InvocationTargetException
   */
  @SuppressWarnings({ "unchecked" })
  public static Object invokeMethod(Class targetClass, Object targetObject,
      String methodName, Class[] argClasses, Object[] argObjects) 
      throws InvocationTargetException {
    try {
      Method method = targetClass.getDeclaredMethod(methodName, argClasses);
      method.setAccessible(true);
      return method.invoke(targetObject, argObjects);
    } catch (NoSuchMethodException e){
      // Should happen only rarely, because most times the
      // specified method should exist. If it does happen, just let
      // the test fail so the programmer can fix the problem.
      throw new AssertionFailedError();
    } catch (SecurityException e) {
      // Should happen only rarely, because the setAccessible(true)
      // should be allowed in when running unit tests. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (IllegalAccessException e) {
      // Should never happen, because setting accessible flag to
      // true. If setting accessible fails, should throw a security
      // exception at that point and never get to the invoke. But
      // just in case, wrap it in a AssertionFailedError and let a
      // human figure it out.
      throw new AssertionFailedError();
    } catch (IllegalArgumentException e) {
      // Should happen only rarely, because usually the right
      // number and types of arguments will be passed. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    }
  }
  
  /**
   * Sets a private field within an object.
   * @param targetObject the object containing the private field
   * @param fieldName the name of the private field
   * @param value the value to set the private field to
   */
  public static void setPrivateField(Object targetObject, String fieldName,
      Object value){
    try {
      Field field = targetObject.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(targetObject, value);
    } catch (IllegalArgumentException e) {
      // Should happen only rarely, because usually the right
      // number and types of arguments will be passed. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (IllegalAccessException e) {
      // Should never happen, because setting accessible flag to
      // true. If setting accessible fails, should throw a security
      // exception at that point and never get to the invoke. But
      // just in case, wrap it in a AssertionFailedError and let a
      // human figure it out.
      throw new AssertionFailedError();
    } catch (SecurityException e) {
      // Should happen only rarely, because the setAccessible(true)
      // should be allowed in when running unit tests. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (NoSuchFieldException e) {
      // Should happen only rarely, because most times the
      // specified field should exist. If it does happen, just let
      // the test fail so the programmer can fix the problem.
      throw new AssertionFailedError();
    }
  }
  
  /**
   * Gets a private field within an object.
   * @param targetObject the object containing the private field
   * @param fieldName the name of the private field
   * @return the value of the private field
   */
  public static Object getPrivateField(Object targetObject, String fieldName) {
    try {
      Field field = targetObject.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(targetObject);
    } catch (IllegalArgumentException e) {
      // Should happen only rarely, because usually the right
      // number and types of arguments will be passed. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (IllegalAccessException e) {
      // Should never happen, because setting accessible flag to
      // true. If setting accessible fails, should throw a security
      // exception at that point and never get to the invoke. But
      // just in case, wrap it in a AssertionFailedError and let a
      // human figure it out.
      throw new AssertionFailedError();
    } catch (SecurityException e) {
      // Should happen only rarely, because the setAccessible(true)
      // should be allowed in when running unit tests. If it does
      // happen, just let the test fail so the programmer can fix
      // the problem.
      throw new AssertionFailedError();
    } catch (NoSuchFieldException e) {
      // Should happen only rarely, because most times the
      // specified field should exist. If it does happen, just let
      // the test fail so the programmer can fix the problem.
      throw new AssertionFailedError();
    }
  }
  

}
