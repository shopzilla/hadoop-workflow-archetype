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
package ${package}.tasklet;

import ${package}.mr.WordCountMapper;
import ${package}.mr.WordCountReducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Required;

import java.net.URI;

public class WordCountTasklet implements Tasklet {
    private static final Log LOG = LogFactory.getLog(WordCountTasklet.class);

    private static final String MAPRED_JOB_POOL = "mapred.job.pool";

    private String hadoopJobName;
    private Configuration configuration;
    private String jobPool;
    private String input;
    private String output;

    @Override
    public final RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // set a nice descriptive job name if none provided
        if (hadoopJobName == null) {
            StringBuilder sb = new StringBuilder();
            StepContext stepContext = chunkContext.getStepContext();
            sb.append("Job: ").append(stepContext.getJobName())
                    .append(", Step: ").append(stepContext.getStepName())
                    .append(" [id: ").append(stepContext.getStepExecution().getJobExecution().getJobId()).append("]");

            hadoopJobName = sb.toString();
        }

        LOG.info("Creating Job: " + hadoopJobName);
        Job job = new Job(configuration, hadoopJobName);

        if (jobPool != null) {
            LOG.info("Setting " + MAPRED_JOB_POOL + " to " + jobPool);
            job.getConfiguration().set(MAPRED_JOB_POOL, jobPool);
        }

        setupJob(job, contribution, chunkContext);

        if (job.waitForCompletion(true)) {
            contribution.setExitStatus(ExitStatus.COMPLETED);
        } else {
            contribution.setExitStatus(ExitStatus.FAILED);
        }
        return RepeatStatus.FINISHED;
    }

    public void setupJob(Job job, StepContribution contribution, ChunkContext chunkContext) throws Exception {

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outDir = new Path(output);
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            fs.delete(outDir);
        } catch (Exception e) {
            System.err.println("Unable to remove existing output directory.");
        }
        FileOutputFormat.setOutputPath(job, outDir);
    }

    @Required
    public void setHadoopJobName(String hadoopJobName) {
        this.hadoopJobName = hadoopJobName;
    }

    @Required
    public void setConfiguration(Configuration configuration) {
        this.configuration = new Configuration(configuration);
    }

    @Required
    public void setInput(String input) {
        this.input = input;
    }

    @Required
    public void setOutput(String output) {
        this.output = output;
    }

    @Required
    public void setJobPool(String jobPool) {
        this.jobPool = jobPool;
    }

}