# Introduction #

`TestUtilities.java` allows for testing of private methods and values. It is meant as a tool for use with the test framework. The argument whether or not testing private methods and values is a good idea is beyond the scope of this wiki.


# Details #

`TestUtilities.java` provides four methods for manipulation of private items:
  * `invokeMethod(Class, String, Class[], Object[])` - calls a private void method
  * `invokeMethod(Class, Object, String, Class[], Object[]` - calls a private method and returns its value
  * `setPrivateField(Object, String, Object)` - sets a private field
  * `getPrivateField(Object, String)` - gets a private field

These methods can be used as-is, however it is easier to use them in conjunction with a convenience method that wraps the above for a specific use in a test.

For example, let's say we have the following `SomeClass.java` and we want to test `convertAndAdd(int intInt, String stringInt)`:
```
public class SomeClass {

  private static int convertAndAdd(int intInt, String stringInt){
    return intInt + Integer.parseInt(stringInt);
  }

}
```

We could write the following test (JUnit4):
```
@Test
public void testSomeClassConvertAndAddIntString() throws InvocationTargetException{
  int shouldBeFour = 4;
  int intTwo = 2;
  String stringTwo = "2";

  Class[] argClasses = {int.class, String.class};
  Object[] argObject = {intTwo, stringTwo};

  assertTrue(
    "Result of 2 + 2 should be 4",
    (int) TestUtilities.invokeMethod(
        SomeClass.class,
        "convertAndAdd",
        argClasses,
        argObjects) == shouldBeFour);
}
```
We could write that, but that is just ugly. We could be much clearer if instead we wrote a convenience method to wrap the call to `TestUtilities`. Like so:
```
private int invokeConvertAndAdd(int intInt, String stringInt)
    throws InvocationTargetException{

  Class[] argClasses = {int.class, String.class};
  Object[] argObjects = {intInt, stringInt};

  return (int) TestUtilities.invokeMethod(
      SomeClass.class,
      "convertAndAdd",
      argClasses,
      argObjects);
}
```
Now we are in a position to clean up the test and make it look like it makes sense:
```
@Test
public void testSomeClassConvertAndAddIntString() throws InvocationTargetException{
  int shouldBeFour = 4;
  int intTwo = 2;
  String stringTwo = "2";

  assertTrue(
    "Result of 2 + 2 should be 4",
    invokeConvertAndAdd(intTwo, stringTwo) == shouldBeFour);
}
```

# References #
  * http://www.artima.com/suiterunner/private3.html