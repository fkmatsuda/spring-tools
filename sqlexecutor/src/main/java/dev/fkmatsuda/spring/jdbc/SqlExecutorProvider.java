/*
 Copyright (c) 2022 fkmatsuda <fabio@fkmatsuda.dev>

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package dev.fkmatsuda.spring.jdbc;

import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import dev.fkmatsuda.spring.common.FileUtils;

@AllArgsConstructor
@Component
public class SqlExecutorProvider {

    private final ApplicationContext context;

    public static class SqlExecutor {

        private static final Pattern QUERY_COUNT_PATTERN = Pattern.compile("(^\\s*select\\s+)(.*?)(\\s+from\\s+.*)($)",
                Pattern.DOTALL | Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
        private static final Pattern ORDER_LIMIT_PATTERN = Pattern.compile(
                "(^.*?)(?>(\\s+order\\s+by)|(\\s+limit)|(\\s+offset))(\\s+.*)($)",
                Pattern.DOTALL | Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

        private final String sql;
        private final NamedParameterJdbcTemplate jdbcTemplate;
        private final SqlExecutorProvider provider;
        private final DataSource dataSource;

        private MapSqlParameterSource parameters = null;

        public <P> SqlExecutor setParameter(String name, P value) {

            if (Objects.isNull(parameters)) {
                this.parameters = new MapSqlParameterSource(name, value);
            } else {
                this.parameters.addValue(name, value);
            }

            return this;

        }

        public <R> List<R> query(RowMapper<R> rowMapper) {
            if (Objects.nonNull(parameters)) {
                return this.jdbcTemplate.query(sql, parameters, rowMapper);
            }
            return this.jdbcTemplate.query(sql, rowMapper);
        }

        public <R> R query(ResultSetExtractor<R> extractor) {
            if (Objects.nonNull(parameters)) {
                return this.jdbcTemplate.query(sql, parameters, extractor);
            }
            return this.jdbcTemplate.query(sql, extractor);
        }

        public void update() throws SqlException {
            checkParameters();

            this.jdbcTemplate.update(sql, parameters);
        }

        public void executeDDL() throws SqlException {

            try (Connection conn = this.dataSource.getConnection()) {
                conn.setAutoCommit(true);
                try (Statement st = conn.createStatement()) {
                    String[] ddlCommands = sql.split(";");
                    for (String ddlCommand : ddlCommands) {
                        if (ddlCommand.trim().isEmpty()) {
                            continue;
                        }
                        st.execute(ddlCommand);
                    }
                }
            } catch (SQLException e) {
                throw new SqlException(e);
            }
        }

        private void checkParameters() throws RequiredValueException {

            if (Objects.isNull(parameters)) {
                throw new RequiredValueException("Parameters is required for this operation");
            }

        }

        private SqlExecutor(SqlExecutorProvider provider, DataSource dataSource, String sql) {
            super();
            this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
            this.sql = sql;
            this.provider = provider;
            this.dataSource = dataSource;
        }

        public String queryForString() {
            return query(ResultSetExtractorFactory.stringExtractor());
        }

        public List<Long> queryForLongList() {
            return query(ResultSetExtractorFactory.longListMapper());
        }

        public Long queryForLong() {
            return query(ResultSetExtractorFactory.longExtractor());
        }

        public Long count() throws InvalidArgumentException {
            Matcher countMatcher = QUERY_COUNT_PATTERN.matcher(sql);
            if (!countMatcher.matches()) {
                throw new InvalidArgumentException("Cannot count current query result");
            }
            String sqlCount = countMatcher.replaceAll("$1count(*)$3");
            Matcher orderLimitMatcher = ORDER_LIMIT_PATTERN.matcher(sqlCount);
            if (orderLimitMatcher.matches()) {
                sqlCount = orderLimitMatcher.replaceAll("$1$4");
            }
            SqlExecutor countExecutor = provider.forSql(sqlCount);
            if (Objects.nonNull(parameters)) {
                countExecutor.parameters = new MapSqlParameterSource(parameters.getValues());
            }
            return countExecutor.queryForLong();
        }

        public BigDecimal queryForBigDecimal() {
            return query(ResultSetExtractorFactory.bigDecimalExtractor());
        }

        public Boolean queryForBoolean() {
            return query(ResultSetExtractorFactory.booleanExtractor());
        }

        public Integer queryForInt() {
            return query(ResultSetExtractorFactory.integerExtractor());
        }

    }

    public SqlExecutor forSql(String sql) {
        return new SqlExecutor(this, context.getBean(DataSource.class), sql);
    }

    public SqlExecutor loadSql(File sqlFile) throws IOException {
        String sql = FileUtils.readToString(sqlFile);
        return forSql(sql);
    }

}
