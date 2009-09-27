import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.mindash.datastore.DatastoreHelperTest;
import com.mindash.datastore.MindashDatastoreServiceImplTest;

@RunWith(Suite.class)
@Suite.SuiteClasses( {
  DatastoreHelperTest.class,
  MindashDatastoreServiceImplTest.class}
)
public class AllTests {
}
