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

package io.questdb.cairo;

import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.vm.VmUtils.STRING_LENGTH_BYTES;
import static io.questdb.cairo.replication.TableReplicationStreamHeaderSupport.SYMBOL_META_BLOCK_SIZE;

public class FileTableStructure implements TableStructure {
    private long tableMetaFileAddress;
    private long tableMetaFileSize;
    private CharSequence tableName;
    private int defaultSymbolCapacity;
    private DirectCharSequence tempCharSequence = new DirectCharSequence();
    private int columnNameIndex = Integer.MAX_VALUE;
    private long columnNamesOffset = 0;
    private long symbolBlockAddress;
    private long symbolBlockHi;

    public FileTableStructure ofTableMeta(long tableMetaFileAddress, long tableMetaFileSize, CharSequence tableName, int defaultSymbolCapacity) {
        this.tableMetaFileAddress = tableMetaFileAddress;
        this.tableMetaFileSize = tableMetaFileSize;
        this.tableName = tableName;
        this.defaultSymbolCapacity = defaultSymbolCapacity;
        columnNameIndex = Integer.MAX_VALUE;
        columnNamesOffset = 0;
        symbolBlockAddress = 0;
        symbolBlockHi = 0;
        return this;
    }

    public FileTableStructure ofSymbolBlock(long symbolBlockAddress, long symbolBlockSize) {
        this.symbolBlockAddress = symbolBlockAddress;
        this.symbolBlockHi = symbolBlockAddress + symbolBlockSize;
        return this;
    }

    @Override
    public int getColumnCount() {
        return Unsafe.getUnsafe().getInt(tableMetaFileAddress + META_OFFSET_COUNT);
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        if (columnNameIndex > columnIndex) {
            columnNameIndex = 0;
            columnNamesOffset = TableUtils.META_OFFSET_COLUMN_TYPES + getColumnCount() * META_COLUMN_DATA_SIZE;
        } else if (columnNameIndex == columnIndex) {
            return getStr(columnNamesOffset);
        }

        CharSequence columnName = null;
        for(; columnNameIndex <= columnIndex; columnNameIndex++) {
            assert columnNamesOffset < tableMetaFileSize - STRING_LENGTH_BYTES;
            columnName = getStr(columnNamesOffset);
            columnNamesOffset += STRING_LENGTH_BYTES + columnName.length() * Character.BYTES;
        }
        assert columnName != null;
        return columnName;
    }

    private CharSequence getStr(long offset) {
        return getStr0(offset, tempCharSequence);
    }

    public final CharSequence getStr0(long offset, DirectCharSequence view) {
        final int len = Unsafe.getUnsafe().getInt(tableMetaFileAddress + offset);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }

        if (len == 0) {
            return "";
        }

        return view.of(tableMetaFileAddress + offset + STRING_LENGTH_BYTES, tableMetaFileAddress + offset + STRING_LENGTH_BYTES + len * Character.BYTES);
    }

    @Override
    public int getColumnType(int columnIndex) {
        final long offset = META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE;
        return Unsafe.getUnsafe().getByte(tableMetaFileAddress + offset);
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return Unsafe.getUnsafe().getInt(tableMetaFileAddress + META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 9);
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return (getColumnFlags(columnIndex) & META_FLAG_BIT_INDEXED) != 0;
    }

    @Override
    public boolean isSequential(int columnIndex) {
        return (getColumnFlags(columnIndex) & META_FLAG_BIT_SEQUENTIAL) != 0;
    }

    @Override
    public int getPartitionBy() {
        return Unsafe.getUnsafe().getInt(tableMetaFileAddress + META_OFFSET_PARTITION_BY);
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        long offset = symbolBlockAddress + columnNameIndex * SYMBOL_META_BLOCK_SIZE + Integer.BYTES;
        if (offset >= symbolBlockHi) return true;
        return Unsafe.getUnsafe().getByte(symbolBlockAddress + columnNameIndex * SYMBOL_META_BLOCK_SIZE + Integer.BYTES) != 0;
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        long offset = symbolBlockAddress + columnNameIndex * SYMBOL_META_BLOCK_SIZE;
        if (offset >= symbolBlockHi) return defaultSymbolCapacity;
        return Unsafe.getUnsafe().getInt(symbolBlockAddress + columnNameIndex * SYMBOL_META_BLOCK_SIZE);
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTimestampIndex() {
        return Unsafe.getUnsafe().getInt(tableMetaFileAddress + META_OFFSET_TIMESTAMP_INDEX);
    }

    private long getColumnFlags(int columnIndex) {
        return Unsafe.getUnsafe().getLong(tableMetaFileAddress + META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 1);
    }

    @Override
    public int getMaxUncommittedRows() {
        return Unsafe.getUnsafe().getInt(tableMetaFileAddress + META_OFFSET_MAX_UNCOMMITTED_ROWS);
    }

    @Override
    public long getCommitLag() {
        return Unsafe.getUnsafe().getLong(tableMetaFileAddress + META_OFFSET_COMMIT_LAG);
    }
}
