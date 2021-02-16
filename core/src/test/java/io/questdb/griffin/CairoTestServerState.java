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
import io.questdb.cairo.replication.MasterReplicationService;
import io.questdb.cairo.replication.SlaveReplicationService;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.IOException;

public class CairoTestServerState implements Closeable {

    private DefaultCairoConfiguration configuration;
    private CairoEngine engine;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl sqlExecutionContext;
    private WorkerPool workerPool;
    private TelemetryJob telemetryJob;
    private TemporaryFolder temp;
    private MasterReplicationService masterReplicationService;

    public CairoTestServerState(
            DefaultCairoConfiguration configuration,
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContextImpl sqlExecutionContext,
            WorkerPool workerPool,
            TelemetryJob telemetryJob,
            TemporaryFolder temp, MasterReplicationService masterReplicationService, SlaveReplicationService slaveReplicationService) {

        this.configuration = configuration;
        this.engine = engine;
        this.compiler = compiler;
        this.sqlExecutionContext = sqlExecutionContext;
        this.workerPool = workerPool;
        this.telemetryJob = telemetryJob;
        this.temp = temp;
        this.masterReplicationService = masterReplicationService;
    }

    public void cleanUpData() {
        engine.resetTableId();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (Path path = new Path().of(configuration.getRoot())) {
            Files.rmdir(path.$());
            Files.mkdirs(path.of(configuration.getRoot()).put(Files.SEPARATOR).$(), configuration.getMkDirMode());
        }
    }

    @Override
    public void close() {
        workerPool.halt();
        if (telemetryJob != null) {
            Misc.free(telemetryJob);
        }
        engine.close();
        if (masterReplicationService != null)  masterReplicationService.close();
        temp.delete();
    }

    public CompiledQuery compile(CharSequence sql) throws SqlException {
        return compiler.compile(sql, sqlExecutionContext);
    }

    public CairoEngine getEngine() {
        return engine;
    }
}
