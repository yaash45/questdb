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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.ObjList;

public class MatchSymCharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "~(Ka)";
    }

    @Override
    public Function newInstance(
            ObjList<Function> args,
            int position,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final SymbolFunction value = (SymbolFunction) args.getQuick(0);
        if (value.isSymbolTableStatic()) {
            return new IntMatchFunction(
                    position,
                    value,
                    args.getQuick(1).getChar(null)
            );
        } else {
            return new StrMatchFunction(
                    position,
                    value,
                    args.getQuick(1).getChar(null)
            );
        }
    }

    private static class StrMatchFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final char expected;

        public StrMatchFunction(int position, Function value, char expected) {
            super(position);
            this.value = value;
            this.expected = expected;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getSymbol(rec);
            return cs != null && Chars.indexOf(cs, expected) != -1;
        }
    }

    private static class IntMatchFunction extends BooleanFunction implements UnaryFunction {
        private final SymbolFunction value;
        private final char expected;
        private final IntHashSet symbolIDs = new IntHashSet();
        private int symbolCount = 0;

        public IntMatchFunction(int position, SymbolFunction value, char expected) {
            super(position);
            this.value = value;
            this.expected = expected;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            return symbolIDs.contains(value.getInt(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            int sz = symbolTable.size();
            if (this.symbolCount < sz) {
                for (int i = this.symbolCount; i < sz; i++) {
                    if (Chars.indexOf(symbolTable.valueOf(i), expected) != -1) {
                        symbolIDs.add(i);
                    }
                }
                this.symbolCount = sz;
            }
        }
    }
}
