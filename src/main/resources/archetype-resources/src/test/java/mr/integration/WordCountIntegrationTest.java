#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc.
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */
package ${package}.mr.integration;

import com.google.common.base.Function;
import com.shopzilla.hadoop.mapreduce.MiniMRClusterContext;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertFalse;


/**
 * Integration test for SEN fetcher job stream.
 */
@ContextConfiguration(locations = "classpath:/base-test-context.xml")
@RunWith(value = SpringJUnit4ClassRunner.class)
public class WordCountIntegrationTest extends BaseMiniMRClusterTest {


    @Resource(name = "wordCountJob")
    private Job job;

    @Resource(name = "jobLauncher")
    JobLauncher jobLauncher;

    @Value("${symbol_dollar}{WORDCOUNT_INPUT_FOLDER}/")
    private String wordCountInputFolder;

    @Value("${symbol_dollar}{WORDCOUNT_OUTPUT_FOLDER}/")
    private String wordCountOutputFolder;

    @Value("${symbol_dollar}{WORDCOUNT_FILTERED_OUTPUT_FOLDER}/")
    private String wordCountFilteredOutputFolder;

    @Value("${symbol_dollar}{WORDCOUNT_PRODUCTION_FOLDER}/")
    private String wordCountProductionFolder;

    @Value("${symbol_dollar}{WORDCOUNT_MINIMUM_THRESHOLD}")
    private Integer wordCountMinimumThreshold;

    @Resource(name = "miniMrClusterContext")
    private MiniMRClusterContext miniMrClusterContext;


    private static final HashMap<String, Object> jobParameters = new HashMap<String, Object>();

    private static final int NUM_UNIQUE_WORDS = 81;
    private static final int NUM_WORDS_ABOVE_WORDCOUNT_THRESHOLD = 20;

    @Test
    public void testWordCount() throws Exception {

        jobLauncher.run(job, new JobParametersBuilder().toJobParameters());

        /**
         * Verifying 'wordCountStep'
         */
        assertPathContainsNumRows("There should be one row per unique word in the MapReduce output",
                new Path(wordCountOutputFolder),
                NUM_UNIQUE_WORDS);

        /**
         * Verifying 'filterCountsStep'
         */
        final AtomicBoolean countsbelowThreshold = new AtomicBoolean(false);
        getMiniMRClusterContext().processData(new Path(wordCountProductionFolder), new Function<String, Void>() {
            @Override
            public Void apply(String input) {
                // If any word in the filtered output has less rows than "filter_count.pig"'s min threshold
                if(Integer.parseInt(input.split("${symbol_escape}${symbol_escape}t")[1]) < wordCountMinimumThreshold) {
                    // set a boolean that will fail an assertion
                    countsbelowThreshold.set(true);
                }
                return null;
            }
        });
        assertFalse("Ensure that all rows have been filtered by the pig script", countsbelowThreshold.get());

        /**
         * Verifying 'publishFilteredDataToProductionFolderStep'
         */
        assertPathContainsNumRows("There should be only as many rows in the published production output as there are words above threshold",
                new Path(wordCountProductionFolder),
                NUM_WORDS_ABOVE_WORDCOUNT_THRESHOLD);

    }

    @Override
    protected MiniMRClusterContext getMiniMRClusterContext() {
        return miniMrClusterContext;
    }
}
