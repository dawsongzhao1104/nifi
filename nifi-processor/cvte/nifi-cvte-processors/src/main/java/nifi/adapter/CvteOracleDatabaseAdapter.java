/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifi.adapter;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
//import org.apache.nifi.processors.standard.db.DatabaseAdapter;

/**
 * A DatabaseAdapter that generates Oracle-compliant SQL.
 */
public class CvteOracleDatabaseAdapter implements CvteDatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsInsertIgnore() {
        return true;
    }

    @Override
    public String getDescription() {
        return "Generates Oracle compliant SQL";
    }


    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        Preconditions.checkArgument(!StringUtils.isEmpty(table), "Table name cannot be null or blank");
        Preconditions.checkArgument(columnNames != null && !columnNames.isEmpty(), "Column names cannot be null or empty");
        Preconditions.checkArgument(uniqueKeyColumnNames != null && !uniqueKeyColumnNames.isEmpty(), "Key column names cannot be null or empty");

        String parameterizedUsingCause = columnNames.stream()
                .map(columnName -> "? "+ columnName)
                .collect(Collectors.joining(", \n\t"));

        StringBuilder usingSelectCause = new StringBuilder(" USING ( \n\t SELECT \n\t");
        usingSelectCause.append(parameterizedUsingCause)
                .append("\n\t FROM dual) t2 ");


        //更新与插入字段 需要排除主键
        List<String> columnForUpdateOrInsert = columnNames.stream().
                filter(e-> !uniqueKeyColumnNames.contains(e))
                .collect(Collectors.toList());

        String updateSetCause = columnForUpdateOrInsert.stream()
                .map(columnName -> "t1." + columnName + " = t2." + columnName)
                .collect(Collectors.joining(",\n\t "));

        String insertCause = columnForUpdateOrInsert.stream()
                .map(columnName -> "t1." + columnName)
                .collect(Collectors.joining(", \n\t"));

        String insertValues = columnForUpdateOrInsert.stream()
                .map(columnName -> "t2." + columnName)
                .collect(Collectors.joining(", \n\t "));

        String onClause = uniqueKeyColumnNames.stream()
                .map(columnName -> "t1." + columnName + " = t2." + columnName)
                .collect(Collectors.joining(" AND "));

        StringBuilder statementStringBuilder = new StringBuilder("MERGE INTO ")
                .append(table).append(" t1 ")
                .append(usingSelectCause)
                .append(" ON ")
                .append(" (").append(onClause).append(") ")
                .append(" \n\t WHEN MATCHED THEN UPDATE SET \n\t")
                .append(updateSetCause)
                .append(" \n\t WHEN NOT  MATCHED THEN INSERT (")
                .append(insertCause).append(" ) \n\t ")
                .append(" VALUES\n\t ")
                .append(" (").append(insertValues).append(") ");

        return statementStringBuilder.toString();
    }

    @Override
    public String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        Preconditions.checkArgument(!StringUtils.isEmpty(table), "Table name cannot be null or blank");
        Preconditions.checkArgument(columnNames != null && !columnNames.isEmpty(), "Column names cannot be null or empty");
        Preconditions.checkArgument(uniqueKeyColumnNames != null && !uniqueKeyColumnNames.isEmpty(), "Key column names cannot be null or empty");

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        String conflictClause = "(" + uniqueKeyColumnNames.stream().collect(Collectors.joining(", ")) + ")";

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON CONFLICT ")
                .append(conflictClause)
                .append(" DO NOTHING");
        return statementStringBuilder.toString();
    }




    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        final StringBuilder query = new StringBuilder();
        boolean nestedSelect = (limit != null || offset != null) && StringUtils.isEmpty(columnForPartitioning);
        if (nestedSelect) {
            // Need a nested SELECT query here in order to use ROWNUM to limit the results
            query.append("SELECT ");
            if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
                query.append("*");
            } else {
                query.append(columnNames);
            }
            query.append(" FROM (SELECT a.*, ROWNUM rnum FROM (");
        }

        query.append("SELECT ");
        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
            if (!StringUtils.isEmpty(columnForPartitioning)) {
                query.append(" AND ");
                query.append(columnForPartitioning);
                query.append(" >= ");
                query.append(offset != null ? offset : "0");
                if (limit != null) {
                    query.append(" AND ");
                    query.append(columnForPartitioning);
                    query.append(" < ");
                    query.append((offset == null ? 0 : offset) + limit);
                }
            }
        }
        if (!StringUtils.isEmpty(orderByClause) && StringUtils.isEmpty(columnForPartitioning)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }
        if (nestedSelect) {
            query.append(") a");
            long offsetVal = 0;
            if (offset != null) {
                offsetVal = offset;
            }
            if (limit != null) {
                query.append(" WHERE ROWNUM <= ");
                query.append(offsetVal + limit);
            }
            query.append(") WHERE rnum > ");
            query.append(offsetVal);
        }

        return query.toString();
    }

    @Override
    public String getTableAliasClause(String tableName) {
        return tableName;
    }
}
