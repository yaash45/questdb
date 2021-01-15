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

public class DefaultSlaveReplicationConfiguration implements SlaveReplicationConfiguration {
    private final int newConnectionQueueLen;
    private final int instructionQueueLen;
    private final int connectionCallbackQueueLen;
    private final boolean enabled;
    private final NetworkFacade nf;

    public DefaultSlaveReplicationConfiguration(){
        newConnectionQueueLen = 2;
        instructionQueueLen = 2;
        connectionCallbackQueueLen = 8;
        enabled = true;
        nf = NetworkFacadeImpl.INSTANCE;
    }

    public DefaultSlaveReplicationConfiguration(int newConnectionQueueLen, int instructionQueueLen, int connectionCallbackQueueLen, boolean enabled, NetworkFacade nf) {
        this.newConnectionQueueLen = newConnectionQueueLen;
        this.instructionQueueLen = instructionQueueLen;
        this.connectionCallbackQueueLen = connectionCallbackQueueLen;
        this.enabled = enabled;
        this.nf = nf;
    }

    @Override
    public int getConnectionCallbackQueueLen() {
        return connectionCallbackQueueLen;
    }

    @Override
    public int getInstructionQueueLen() {
        return instructionQueueLen;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return nf;
    }

    @Override
    public int getNewConnectionQueueLen() {
        return newConnectionQueueLen;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
}
