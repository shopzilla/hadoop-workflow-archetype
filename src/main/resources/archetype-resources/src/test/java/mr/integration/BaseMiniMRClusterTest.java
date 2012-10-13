#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.mr.integration;

import com.google.common.base.Function;
import com.shopzilla.hadoop.mapreduce.MiniMRClusterContext;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.fs.FsShell;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author look
 * @since 6/26/12
 */
public abstract class BaseMiniMRClusterTest {

    protected abstract MiniMRClusterContext getMiniMRClusterContext();

    private FsShell fsh;

    protected FsShell getFsShell() {
        if (fsh == null) {
            fsh = new FsShell(getMiniMRClusterContext().getConfiguration());
        }
        return fsh;
    }

    protected int countRowsInHdfsPath(Path inputPath) throws IOException {
        final AtomicInteger inputRecordCount = new AtomicInteger(0);
        getMiniMRClusterContext().processData(inputPath, new Function<String, Void>() {
            @Override
            public Void apply(String input) {
                System.out.println(input);
                inputRecordCount.incrementAndGet();
                return null;
            }
        });
        return inputRecordCount.intValue();
    }

    protected void assertPathContainsNumRows(String message, Path inputPath, int numExpectedRows) throws IOException {
        // Make sure the end record count is equal to the input size
        assertEquals(message, numExpectedRows, countRowsInHdfsPath(inputPath));
    }

    protected void assertPathContainsNumRowsGreaterThan(String message, Path inputPath, int minimumNumExpectedRows) throws IOException {
        // Make sure the end record count is equal to the input size
        assertTrue(message, countRowsInHdfsPath(inputPath) > minimumNumExpectedRows);
    }

    protected void assertPathContainsNumRowsLessThan(String message, Path inputPath, int maximumNumExpectedRows) throws IOException {
        // Make sure the end record count is equal to the input size
        assertTrue(message, countRowsInHdfsPath(inputPath) < maximumNumExpectedRows);
    }
}
