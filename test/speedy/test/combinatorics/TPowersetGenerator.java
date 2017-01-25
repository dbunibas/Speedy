package speedy.test.combinatorics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.utility.SpeedyUtility;
import speedy.utility.combinatorics.GenericListGeneratorIterator;

public class TCombinatorics extends TestCase {
    
    private static Logger logger = LoggerFactory.getLogger(TCombinatorics.class);
    
    public TCombinatorics(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void test() {
        List<List<Integer>> lists = new ArrayList<List<Integer>>();
        List<Integer> list1 = Arrays.asList(new Integer[]{1, 2, 3});
        List<Integer> list2 = Arrays.asList(new Integer[]{1});
        List<Integer> list3 = Arrays.asList(new Integer[]{1, 2});
        lists.add(list1);
        lists.add(list2);
        lists.add(list3);
        GenericListGeneratorIterator<Integer> iterator = new GenericListGeneratorIterator<Integer>(lists);
        while(iterator.hasNext()) {
            logger.info(iterator.next().toString());
        }
    }
}
