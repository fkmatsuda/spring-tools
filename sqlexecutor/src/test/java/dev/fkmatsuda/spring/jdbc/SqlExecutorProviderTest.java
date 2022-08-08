package dev.fkmatsuda.spring.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import dev.fkmatsuda.spring.jdbc.SqlExecutorProvider.SqlExecutor;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SqlExecutorProviderTestConfig.class})
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class SqlExecutorProviderTest {

    @Autowired
    private SqlExecutorProvider sqlExecutorProvider;

    @Test
    public void testValidQuery() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test (id INTEGER);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test (id) VALUES (:id)");
        for (int i = 0; i < 16; i++) {
            insertQuery.setParameter("id", i).update();
        }
        SqlExecutor executor = sqlExecutorProvider.forSql("select * from test order by id");
        Long cnt = executor.count();
        assertEquals(16, cnt.intValue());

        List<Long> ids = executor.queryForLongList();

        for (int i = 0; i < 16; i++) {
            assertEquals(i, ids.get(i).intValue());
        }

    }

    @Test()
    public void testFailedParameter() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test2 (id INTEGER);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test2 (id) VALUES (:id)");
        RequiredValueException e = assertThrows(RequiredValueException.class, 
        () -> insertQuery.update());
        assertEquals("Parameters is required for this operation", e.getMessage());
    }

    @Test()
    public void testCountInvalidQuery() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("CREATE TABLE test3 (id INTEGER);");
        InvalidArgumentException e = assertThrows(InvalidArgumentException.class, 
        () -> executor.count());
        assertEquals("Cannot count current query result", e.getMessage());
    }

    @Test()
    public void testSqlException() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("select from voyts");
        SqlException e = assertThrows(SqlException.class, 
        () -> executor.update());
    }
    
}
