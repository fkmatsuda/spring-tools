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
        assertNotNull(e);
    }
    
    @Test
    public void testValidQueryWithParams() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test4 (id INTEGER);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test4 (id) VALUES (:id)");
        for (int i = 0; i < 16; i++) {
            insertQuery.setParameter("id", i).update();
        }
        SqlExecutor executor = sqlExecutorProvider.forSql("select * from test4 where id < :id_limit order by id");
        executor.setParameter("id_limit", 4);
        Long cnt = executor.count();
        assertEquals(4, cnt.intValue());

        List<Long> ids = executor.queryForLongList();

        for (int i = 0; i < 4; i++) {
            assertEquals(i, ids.get(i).intValue());
        }

    }

    @Test()
    public void testDDLException() {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("CREATE voyts;;");
        SqlException e = assertThrows(SqlException.class, 
        () -> executor.executeDDL());
        assertNotNull(e);
    }

    @Test()
    public void testQueryForString() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test5 (id INTEGER, str text);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test5 (id, str) VALUES (:id, :str)");
        for (int i = 0; i < 16; i++) {
            insertQuery.setParameter("id", i).setParameter("str", String.format("str: %d", i)).update();
        }
        SqlExecutor executor = sqlExecutorProvider.forSql("select str from test5 where id = :id");
        String str = executor.setParameter("id", 2).queryForString();

        assertEquals("str: 2", str);
    }

}
