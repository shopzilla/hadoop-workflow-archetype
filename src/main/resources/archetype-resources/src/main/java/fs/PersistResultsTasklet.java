#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */
package ${package}.fs;
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
import org.apache.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Required;

import static java.lang.String.format;

/**
 * If 'fileToTest' does not exist, fail out of the job flow.
 * Otherwise, clean up dest folder and then move the contents of source foler into dest.
 *
 * @author look
 * @since 6/25/12
 */
public class PersistResultsTasklet extends FileSystemBaseTasklet {
    private static final Logger LOG = Logger.getLogger(PersistResultsTasklet.class);

    private static final Boolean CHECK_IF_ZERO_BYTES = false;
    private static final Boolean CHECK_IF_EXISTS = true;
    private static final Boolean SKIP_TRASH = true;

    private String testPath;
    private Boolean shouldTestForDirectory;
    private String sourcePath;
    private String destPath;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        LOG.info(format("Checking for existence of %s ( directory ? %s )", testPath, shouldTestForDirectory));
        if(getFsShell().test(CHECK_IF_EXISTS, CHECK_IF_ZERO_BYTES, shouldTestForDirectory, testPath)) {
            LOG.info(format("Moving %s to %s", sourcePath, destPath));
            if(!getFsShell().test(destPath)) {
                getFsShell().mkdir(destPath);
            }
            getFsShell().rmr(SKIP_TRASH, destPath);

            getFsShell().mv(sourcePath, destPath);

            stepContribution.setExitStatus(ExitStatus.COMPLETED);
        } else {
            stepContribution.setExitStatus(ExitStatus.FAILED);
        }

        return RepeatStatus.FINISHED;
    }

    @Required
    public void setShouldTestForDirectory(Boolean shouldTestForDirectory) {
        this.shouldTestForDirectory = shouldTestForDirectory;
    }

    @Required
    public void setTestPath(String testPath) {
        this.testPath = testPath;
    }

    @Required
    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    @Required
    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }
}
