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

package io.questdb.cutlass.line;

import static io.questdb.cutlass.line.LineProtoLexer.ReturnState.OK;

public class CairoLineProtoParser implements LineProtoParser {
    private final LineProtoLexer lexer;
    private LineProtoParserListener listener;

    public CairoLineProtoParser(LineProtoLexer lexer) {
        this.lexer = lexer;
    }

    public CairoLineProtoParser(LineProtoLexer lexer, LineProtoParserListener listener) {
        this.listener = listener;
        this.lexer = lexer;
    }

    public void withListener(LineProtoParserListener listener) {
        this.listener = listener;
    }

    @Override
    public void clear() {
        lexer.clear();
    }

    @Override
    public void commitAll(int commitMode) {
        listener.commitAll(commitMode);
    }

    @Override
    public void parse(long lo, long hi) {
        while (lo < hi) {
            LineProtoLexer.ReturnState state = lexer.parse(lo, hi);
            lo = lexer.getLastPosition();
            listen(state);
            if (state == OK) {
                // Sometimes code cannot parse further because not enough of UTF8 chars in the buffer.
                return;
            }
        }
    }

    private void listen(LineProtoLexer.ReturnState state) {
        switch (state) {
            case ON_EVENT:
                listener.onEvent(lexer.getToken(), lexer.getState(), lexer.getCharCache());
                break;
            case ON_EOL:
                listener.onEvent(lexer.getToken(), lexer.getState(), lexer.getCharCache());
                listener.onLineEnd(lexer.getCharCache());
                break;
            case ON_ERROR:
                listener.onError(lexer.getErrorPosition(), lexer.getState(), lexer.getErrorCode());
                break;
        }
    }

    @Override
    public void parseLast() {
        LineProtoLexer.ReturnState state = lexer.parseLast();
        listen(state);
    }
}
