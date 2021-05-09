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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.ObjList;

import java.util.regex.Matcher;

public class NotMatchStrStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "!~(Ss)";
    }

    @Override
    public Function newInstance(
            ObjList<Function> args,
            int position,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return RegexFunctionUtils.getRegexFunction(args, position, MatchFunction.CONSTRUCTOR);
    }

    private static class MatchFunction extends BooleanFunction implements UnaryFunction {
        private final static RegexFunctionConstructor CONSTRUCTOR = MatchFunction::new;
        private final Function arg;
        private final Matcher matcher;

        public MatchFunction(int position, Function arg, Matcher matcher) {
            super(position);
            this.arg = arg;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return cs == null || !matcher.reset(cs).find();
        }
    }
}
