#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * @author look
 * @since 6/25/12
 */
public abstract class FileSystemBaseTasklet implements Tasklet {
    private FsShell fsShell;

    public FsShell getFsShell() {
        return fsShell;
    }

    @Required
    public void setFsShell(FsShell fsShell) {
        this.fsShell = fsShell;
    }
}