package dev.fkmatsuda.spring.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
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
    void testValidQuery() throws SqlException, InvalidArgumentException {
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
    void testFailedParameter() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test2 (id INTEGER);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test2 (id) VALUES (:id)");
        RequiredValueException e = assertThrows(RequiredValueException.class, 
        () -> insertQuery.update());
        assertEquals("Parameters is required for this operation", e.getMessage());
    }

    @Test()
    void testCountInvalidQuery() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("CREATE TABLE test3 (id INTEGER);");
        InvalidArgumentException e = assertThrows(InvalidArgumentException.class, 
        () -> executor.count());
        assertEquals("Cannot count current query result", e.getMessage());
    }

    @Test()
    void testSqlException() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("select from voyts");
        SqlException e = assertThrows(SqlException.class, 
        () -> executor.update());
        assertNotNull(e);
    }
    
    @Test
    void testValidQueryWithParams() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test4 (id INTEGER);").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql("INSERT INTO test4 (id) VALUES (:id)");
        for (int i = 0; i < 16; i++) {
            insertQuery.setParameter("id", i).update();
        }
        SqlExecutor executor = sqlExecutorProvider.forSql("select * from test4 where id < :id_limit");
        executor.setParameter("id_limit", 4);
        Long cnt = executor.count();
        assertEquals(4, cnt.intValue());

        List<Long> ids = executor.queryForLongList();

        assertTrue(ids.containsAll(List.of(0L, 1L, 2L, 3L)));

    }

    @Test()
    void testDDLException() {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("CREATE voyts;;");
        SqlException e = assertThrows(SqlException.class, 
        () -> executor.executeDDL());
        assertNotNull(e);
    }

    @Test()
    void testQueryForString() throws SqlException, InvalidArgumentException {
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

    @Test()
    void testQueryForInt() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("select 1");
        Integer id = executor.queryForInt();
        assertEquals(1, id.intValue());
    }

    @Test()
    void testQueryForBoolean() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("select 1 = 1");
        Boolean b = executor.queryForBoolean();
        assertTrue(b);
    }

    @Test()
    void testQueryForBigDecimal() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        SqlExecutor executor = sqlExecutorProvider.forSql("select 1.1");
        BigDecimal b = executor.queryForBigDecimal();
        assertEquals(1.1, b.doubleValue(), 0.00001);
    }

    @Test()
    void testQueryNullValues() throws SqlException, InvalidArgumentException {
        assertNotNull(sqlExecutorProvider);
        sqlExecutorProvider.forSql("CREATE TABLE test6 (" + 
            "id INTEGER, str text, " + 
            "b_value boolean, " + 
            "i_value int4, " + 
            "l_value int8, " +
            "n_value numeric(15,5));").executeDDL();
        SqlExecutor insertQuery = sqlExecutorProvider.forSql(
            "INSERT INTO test6 (id) VALUES (:id)");
        for (int i = 0; i < 16; i++) {
            insertQuery.setParameter("id", i).update();
        }

        Long lValue = sqlExecutorProvider.forSql("select l_value from test6 where id = :id")
            .setParameter("id", 1L).queryForLong();
        assertNull(lValue);

        BigDecimal nValue = sqlExecutorProvider.forSql("select n_value from test6 where id = :id")
            .setParameter("id", 1L).queryForBigDecimal();
        assertNull(nValue);

        String strValue = sqlExecutorProvider.forSql("select str from test6 where id = :id")
            .setParameter("id", 1L).queryForString();
        assertNull(strValue);

        Boolean bValue = sqlExecutorProvider.forSql("select b_value from test6 where id = :id")
            .setParameter("id", 1L).queryForBoolean();
        assertFalse(bValue);

        Integer iValue = sqlExecutorProvider.forSql("select i_value from test6 where id = :id")
            .setParameter("id", 1L).queryForInt();
        assertNull(iValue);

        List<Long> lValues = sqlExecutorProvider.forSql("select l_value from test6").queryForLongList();
        for (Long l : lValues) {
            assertNull(l);
        }


    }

}
