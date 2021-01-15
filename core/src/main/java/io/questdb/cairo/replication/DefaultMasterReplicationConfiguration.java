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

package io.questdb.cairo.replication;

import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DefaultMasterReplicationConfiguration implements MasterReplicationConfiguration {
    private final ObjList<CharSequence> masterIps;
    private final IntList masterPorts;
    private final int backlog;
    private final boolean isEnabled;
    private final NetworkFacade nf;

    public DefaultMasterReplicationConfiguration() {
        masterIps = new ObjList<>(1);
        masterIps.add("0.0.0.0");
        masterPorts = new IntList(1);
        masterPorts.add(9004);
        backlog = 0;
        isEnabled = true;
        nf = NetworkFacadeImpl.INSTANCE;
    }

    public DefaultMasterReplicationConfiguration(ObjList<CharSequence> masterIps, IntList masterPorts, int backlog, boolean isEnabled, NetworkFacade nf) {
        this.masterIps = masterIps;
        this.masterPorts = masterPorts;
        this.backlog = backlog;
        this.isEnabled = isEnabled;
        this.nf = nf;
    }

    @Override
    public int getBacklog() {
        return backlog;
    }

    @Override
    public ObjList<CharSequence> getMasterIps() {
        return masterIps;
    }

    @Override
    public IntList getMasterPorts() {
        return masterPorts;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return nf;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }
}
