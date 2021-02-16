/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.replication.DefaultMasterReplicationConfiguration;
import io.questdb.cairo.replication.DefaultSlaveReplicationConfiguration;
import io.questdb.cairo.replication.MasterReplicationService;
import io.questdb.cairo.replication.SlaveReplicationService;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;

public class EngineTestBuilder {
    private static final Log LOG = LogFactory.getLog(EngineTestBuilder.class);
    private boolean telemetry;
    private TemporaryFolder temp = new TemporaryFolder();
    private int workerCount = 1;
    private boolean masterReplication;
    private boolean slaveReplication;

    public EngineTestBuilder withMasterReplication() {
        masterReplication = true;
        return this;
    }

    public EngineTestBuilder withSlaveReplication() {
        slaveReplication = true;
        return this;
    }

    public EngineTestBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }

    public int getWorkerCount() {
        return this.workerCount;
    }

    public EngineTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public CairoTestServerState build() throws SqlException, IOException {
        final int[] workerAffinity = new int[workerCount];
        Arrays.fill(workerAffinity, -1);
        temp.create();

        final String baseDir = temp.getRoot().getAbsolutePath();
        final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return workerAffinity;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        });

        DefaultCairoConfiguration cairoConfiguration = new DefaultCairoConfiguration(baseDir);
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(cairoConfiguration);
        CairoEngine engine = new CairoEngine(cairoConfiguration);
        SqlCompiler compiler = new SqlCompiler(engine);
        SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();

        TelemetryJob telemetryJob = null;
        if (telemetry) {
            telemetryJob = new TelemetryJob(engine);
        }
        MasterReplicationService masterReplicationService = null;
        if (masterReplication) {
            masterReplicationService = new MasterReplicationService(
                    cairoConfiguration.getFilesFacade(),
                    cairoConfiguration.getRoot(),
                    engine,
                    new DefaultMasterReplicationConfiguration(),
                    workerPool
            );
        }
        SlaveReplicationService slaveReplicationService = null;
        if (slaveReplication) {
            slaveReplicationService = new SlaveReplicationService(
                    cairoConfiguration,
                    engine,
                    workerPool,
                    new DefaultSlaveReplicationConfiguration()
            );
        }

        workerPool.start(LOG);

        return new CairoTestServerState(cairoConfiguration,
                engine,
                compiler,
                sqlExecutionContext,
                workerPool,
                telemetryJob,
                temp,
                masterReplicationService,
                slaveReplicationService);
    }
}
