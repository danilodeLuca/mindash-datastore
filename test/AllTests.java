import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.mindash.datastore.impl.DatastoreHelperImplTest;
import com.mindash.datastore.impl.MindashDatastoreServiceImplTest;

@RunWith(Suite.class)
@Suite.SuiteClasses( {
  DatastoreHelperImplTest.class,
  MindashDatastoreServiceImplTest.class}
)
public class AllTests {
}
