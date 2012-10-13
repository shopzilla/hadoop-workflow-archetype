#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */
package ${package}.mr.unit;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;

import static java.lang.String.format;

/**
 * @author look
 * @since 7/10/12
 */
public class FilterCountsPigUnitTest {

    private Resource filterCountsScript = new ClassPathResource("pig/filter_counts.pig");

    private static final Integer MINIMUM_THRESHOLD = 4;
    private static final String WORDCOUNT_OUTPUT_FOLDER = "wordCountOutputFolder";
    private static final String WORDCOUNT_FILTERED_OUTPUT_FOLDER = "wordCountFilteredOutputFolder";

    @Test
    public void testFilterCounts() throws IOException, ParseException {
        String[] args = {
            "JOB_POOL=scheduled",
            format("WORDCOUNT_OUTPUT_FOLDER=%s", WORDCOUNT_OUTPUT_FOLDER),
            format("WORDCOUNT_FILTERED_OUTPUT_FOLDER=%s", WORDCOUNT_FILTERED_OUTPUT_FOLDER),
            format("WORDCOUNT_MINIMUM_THRESHOLD=%s", MINIMUM_THRESHOLD)
        };

        PigTest test = new PigTest(filterCountsScript.getFile().getAbsolutePath(), args);

        String[] input = {
            "yahoo${symbol_escape}t5",     // above threshold
            "bing${symbol_escape}t4",      // below threshold
            "twitter${symbol_escape}t2",   // below threshold
            "facebook${symbol_escape}t1",  // below threshold
            "linkedin${symbol_escape}t120" // above threshold
        };

        String[] output = {
            "(yahoo,5)",
            "(linkedin,120)",
        };

        test.assertOutput("KEYWORD_COUNTS", input, "FILTERED_KEYWORD_COUNTS", output);
    }
}
