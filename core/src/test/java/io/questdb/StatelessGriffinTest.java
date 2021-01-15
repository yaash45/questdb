///*******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2020 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb;
//
//import io.questdb.cairo.CairoConfiguration;
//import io.questdb.cairo.CairoEngine;
//import io.questdb.cairo.DefaultCairoConfiguration;
//import io.questdb.cairo.replication.MasterReplicationConfiguration;
//import io.questdb.cairo.replication.MasterReplicationService;
//import io.questdb.cairo.security.AllowAllCairoSecurityContext;
//import io.questdb.griffin.SqlCompiler;
//import io.questdb.griffin.SqlExecutionContextImpl;
//import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
//import io.questdb.log.Log;
//import io.questdb.log.LogFactory;
//import io.questdb.std.Files;
//import io.questdb.std.str.Path;
//import org.junit.Assert;
//import org.junit.rules.TemporaryFolder;
//
//import java.io.IOException;
//
//public class StatelessGriffinTest {
//    public static class StaticState {
//        public final Log LOG = LogFactory.getLog(StatelessGriffinTest.class);
//        public Path path = new Path(4096);
//        public CharSequence root;
//        public TemporaryFolder temp;
//        public CairoConfiguration configuration;
//        public CairoEngine engine;
//        public SqlCompiler compiler;
//        public SqlExecutionContextImpl sqlExecutionContext;
//    }
//
//    public static StaticState setUpClass(String folder) throws IOException {
//        StaticState state = new StaticState();
//        state.temp = new TemporaryFolder();
//        state.temp.create();
//        state.root = state.temp.newFolder(folder).getAbsolutePath();
//        state.configuration = new DefaultCairoConfiguration(state.root);
//        state.engine = new CairoEngine(state.configuration);
//        state.compiler = new SqlCompiler(state.engine);
//        state.sqlExecutionContext = new SqlExecutionContextImpl(state.engine, 1)
//                .with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableServiceImpl(state.configuration), null, -1, null);
//        return state;
//    }
//
//    public static void setUpMasterReplication(StaticState masterState, MasterReplicationConfiguration replicationConfiguration) {
//        new MasterReplicationService(masterState.configuration.getFilesFacade(),
//                masterState.root,
//                masterState.engine,
//                replicationConfiguration)
//        masterState.engine
//    }
//
//    public void setUp(StaticState staticState) {
//    }
//
//    public void after(StaticState staticState) {
//        staticState.engine.resetTableId();
//        staticState.engine.releaseAllReaders();
//        staticState.engine.releaseAllWriters();
//        try (Path path = new Path().of(staticState.root)) {
//            Files.rmdir(path.$());
//            Files.mkdirs(path.of(staticState.root).put(Files.SEPARATOR).$(), staticState.configuration.getMkDirMode());
//        }
//    }
//
//    public static void afterClass(StaticState staticState) {
//        staticState.engine.releaseInactive();
//        Assert.assertEquals(0, staticState.engine.getBusyWriterCount());
//        Assert.assertEquals(0, staticState.engine.getBusyReaderCount());
//        staticState.temp.delete();
//    }
//}
