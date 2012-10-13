#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/**
 * Copyright 2012 Shopzilla.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  http://tech.shopzilla.com
 *
 */
package ${package}.driver;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import java.util.HashMap;
import java.util.Map;

/**
 * @author look
 * @since 6/29/12
 */
public class SampleDriver {

    public static final String WORD_COUNT_JOB = "wordCountJob";
    private static final int ARG_INDEX_JOB_NAME = 0;

    private static Job wordCountJob;

    private static JobLauncher jobLauncher;

    private static void initialize() {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/driver-context.xml");

        wordCountJob = (Job) ctx.getBean(WORD_COUNT_JOB);
        jobLauncher = (JobLauncher) ctx.getBean("jobLauncher");
    }

    /**
     * First argument:  Job Name
     */
    public static void main(String[] args) throws JobInstanceAlreadyCompleteException, JobParametersInvalidException, JobRestartException, JobExecutionAlreadyRunningException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Too few arguments");
        }
        initialize();

        jobLauncher.run(wordCountJob, new JobParametersBuilder().toJobParameters());
    }
}