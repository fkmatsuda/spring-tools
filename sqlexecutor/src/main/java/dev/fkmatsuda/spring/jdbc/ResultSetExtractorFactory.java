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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

public class ResultSetExtractorFactory {
    private ResultSetExtractorFactory() {}

    public static ResultSetExtractor<String> stringExtractor() {

        return new ResultSetExtractor<String>() {
            @Override
            public String extractData(ResultSet rs) throws SQLException, DataAccessException {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        };

    }

    public static RowMapper<Long> longListMapper() {
        return new RowMapper<Long>() {
            @Override
            public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
                long value = rs.getLong(1);
                if (rs.wasNull()) {
                    return null;
                }
                return value;
            }
        };
    }

	public static ResultSetExtractor<Long> longExtractor() {
        return new ResultSetExtractor<Long>() {
            @Override
            public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                if (rs.next()) {
                	long value = rs.getLong(1);
                	if (rs.wasNull()) {
                		return null;
                	}
                    return value;
                }
                return null;
            }
        };
	}

	public static ResultSetExtractor<BigDecimal> bigDecimalExtractor() {
		return new ResultSetExtractor<BigDecimal>() {

			@Override
			public BigDecimal extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
                	BigDecimal value = rs.getBigDecimal(1);
                	if (rs.wasNull()) {
                		return null;
                	}
                    return value;
                }
				return null;
			}
			
		};
	}

	public static ResultSetExtractor<Boolean> booleanExtractor() {
		return new ResultSetExtractor<Boolean>() {

			@Override
			public Boolean extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
                	Boolean value = rs.getBoolean(1);
                	if (rs.wasNull()) {
                		return false;
                	}
                    return value;
                }
				return false;
			}
			
		};
	}

	public static ResultSetExtractor<Integer> integerExtractor() {
        return new ResultSetExtractor<Integer>() {
            @Override
            public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
                if (rs.next()) {
                	int value = rs.getInt(1);
                	if (rs.wasNull()) {
                		return null;
                	}
                    return value;
                }
                return null;
            }
        };
	}

}
