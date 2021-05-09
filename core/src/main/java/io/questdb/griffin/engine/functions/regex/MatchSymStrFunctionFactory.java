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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntHashSet;
import io.questdb.std.ObjList;

import java.util.regex.Matcher;

public class MatchSymStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "~(Ks)";
    }

    @Override
    public Function newInstance(
            ObjList<Function> args,
            int position,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        SymbolFunction value = (SymbolFunction) args.getQuick(0);
        if (value.isSymbolTableStatic()) {
            return RegexFunctionUtils.getRegexFunction(args, position, IntMatchFunction.CONSTRUCTOR);
        } else {
            return RegexFunctionUtils.getRegexFunction(args, position, StrMatchFunction.CONSTRUCTOR);
        }
    }

    private static class StrMatchFunction extends BooleanFunction implements UnaryFunction {
        private final static RegexFunctionConstructor CONSTRUCTOR = StrMatchFunction::new;
        private final Function value;
        private final Matcher matcher;

        public StrMatchFunction(int position, Function value, Matcher matcher) {
            super(position);
            this.value = value;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence cs = getArg().getSymbol(rec);
            return cs != null && matcher.reset(cs).find();
        }
    }

    private static class IntMatchFunction extends BooleanFunction implements UnaryFunction {
        private final static RegexFunctionConstructor CONSTRUCTOR = IntMatchFunction::new;
        private final SymbolFunction value;
        private final Matcher matcher;
        private final IntHashSet symbolIDs = new IntHashSet();
        private int symbolCount = 0;

        public IntMatchFunction(int position, Function value, Matcher matcher) {
            super(position);
            this.value = (SymbolFunction) value;
            this.matcher = matcher;
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
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            int sz = symbolTable.size();
            if (symbolCount < sz) {
                for (int i = symbolCount; i < sz; i++) {
                    if (matcher.reset(symbolTable.valueOf(i)).find()) {
                        symbolIDs.add(i);
                    }
                }
                symbolCount = sz;
            }
        }
    }
}
