package com.gl.examination;

import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DatabaseQueryTest {

    private Connection conn;

    @BeforeAll
    public void setup() throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/ltimindtree_db", "root", "");
    }

    @AfterAll
    public void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    // -------------------- TABLE CREATION TESTS --------------------

    @Test
    @DisplayName("Admin table should exist with correct columns")
    public void testAdminTableCreation() throws SQLException {
        assertTableExists("Admin");
        Map<String, String> expectedCols = Map.of(
            "id", "VARCHAR",
            "admin_name", "VARCHAR",
            "email_id", "VARCHAR",
            "password", "VARCHAR"
        );
        assertTableColumns("Admin", expectedCols);
    }

    @Test
    @DisplayName("User table should exist with correct columns")
    public void testUserTableCreation() throws SQLException {
        assertTableExists("User");
        Map<String, String> expectedCols = Map.of(
            "id", "VARCHAR",
            "user_name", "VARCHAR",
            "email_id", "VARCHAR",
            "password", "VARCHAR",
            "gender", "CHAR",
            "city", "VARCHAR",
            "mobile_number", "CHAR",
            "zipcode", "CHAR"
        );
        assertTableColumns("User", expectedCols);
    }

    @Test
    @DisplayName("Product table should exist with correct columns")
    public void testProductTableCreation() throws SQLException {
        assertTableExists("Product");
        Map<String, String> expectedCols = Map.of(
            "id", "VARCHAR",
            "product_name", "VARCHAR",
            "category", "VARCHAR",
            "price", "FLOAT",
            "quantity", "SMALLINT",
            "offers", "VARCHAR",
            "description", "VARCHAR"
        );
        assertTableColumns("Product", expectedCols);
    }

    @Test
    @DisplayName("Payment_method table should exist with correct columns")
    public void testPaymentMethodTableCreation() throws SQLException {
        assertTableExists("Payment_method");
        Map<String, String> expectedCols = Map.of(
            "id", "VARCHAR",
            "account_holder_name", "VARCHAR",
            "account_number", "CHAR",
            "date_of_payment", "DATE"
        );
        assertTableColumns("Payment_method", expectedCols);
    }

    // -------------------- DATA INSERTION TESTS --------------------

    @Test
    @DisplayName("Admin table should have at least one entry")
    public void testAdminTableDataInsertion() throws SQLException {
        String query = "SELECT COUNT(*) FROM Admin";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            assertTrue(rs.next());
            int count = rs.getInt(1);
            assertTrue(count > 0, "Admin table should have at least one record.");
        }
    }

    @Test
    @Order(1)
    @DisplayName("User table should contain Mansi's correct record")
    public void testUserTableDataInsertion() throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT * FROM User WHERE LOWER(user_name) = 'mansi'")) {
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "User with username 'Mansi' should be present");
                assertEquals("mansi@hcl.com", rs.getString("email_id").toLowerCase(), "Incorrect email");
                assertEquals("F", rs.getString("gender").toUpperCase(), "Incorrect gender");
                assertEquals("1111111111", rs.getString("mobile_number"), "Incorrect mobile number");
                assertEquals("123456", rs.getString("zipcode"), "Incorrect zipcode");
            }
        }
    }

    @Test
    @DisplayName("Product table should contain 5 records with valid data")
    public void testProductTableDataInsertion() throws SQLException {
        String countQuery = "SELECT COUNT(*) AS total FROM Product";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            assertTrue(rs.next());
            int total = rs.getInt("total");
            assertEquals(5, total, "Product table should contain 5 records");
        }

        String selectQuery = "SELECT * FROM Product ORDER BY id";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectQuery)) {

            int count = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("product_name");
                String category = rs.getString("category");
                double price = rs.getDouble("price");
                int quantity = rs.getInt("quantity");
                String offers = rs.getString("offers");
                String desc = rs.getString("description");

                switch (id) {
                case 1:
                    assertEquals("Laptop", name);
                    assertEquals("Electronics", category);
                    assertEquals(50000.00, price, 0.01);
                    assertEquals(10, quantity);
                    assertEquals("10%", offers);
                    assertNull(desc);
                    break;

                case 2:
                    assertEquals("Desk", name);
                    assertEquals("Furniture", category);
                    assertEquals(15000.00, price, 0.01);
                    assertEquals(3, quantity);
                    assertEquals("2%", offers);
                    assertEquals("Computer desk", desc);
                    break;

                case 3:
                    assertEquals("Bedsheet", name);
                    assertEquals("HomeCare", category);
                    assertEquals(3000.00, price, 0.01);
                    assertEquals(5, quantity);
                    assertEquals("0%", offers);
                    assertEquals("Cotton", desc);
                    break;

                case 4:
                    assertEquals("Biscuits", name);
                    assertEquals("Grocery", category);
                    assertEquals(45.56, price, 0.01);
                    assertEquals(20, quantity);
                    assertEquals("1%", offers);
                    assertNull(desc);
                    break;

                case 5:
                    assertEquals("EarPhones", name);
                    assertEquals("Electronics", category);
                    assertEquals(1000.00, price, 0.01);
                    assertEquals(3, quantity);
                    assertEquals("0.5%", offers);
                    assertEquals("Wire less", desc);
                    break;

                default:
                    fail("Unexpected product id: " + id);
            }
            count++;
            }
            assertEquals(5, count, "Exactly 5 products should be validated");
        }
    }

    // -------------------- OPTIONAL: QUERY VALIDATION --------------------

    

    @Test
    @DisplayName("Check presence and syntax of ALTER command in queries.sql without execution")
    public void testAlterCommandPresenceInQueriesSql() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("queries.sql");
        assertNotNull(is, "queries.sql file not found in resources folder");

        String sqlContent;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            sqlContent = reader.lines().collect(Collectors.joining("\n")).trim();
        }

        // Extract ALTER command - assume after first semicolon is the ALTER query
        int selectEndIndex = sqlContent.toLowerCase().indexOf(";");
        assertTrue(selectEndIndex > 0, "There must be at least two queries separated by semicolon");

        String alterQuery = sqlContent.substring(selectEndIndex + 1).trim();

        // Regex to check ALTER command (case insensitive)
        String alterRegex = "(?i)^ALTER\\s+TABLE\\s+User\\s+ADD\\s+PRIMARY\\s+KEY\\s*\\(\\s*id\\s*\\)\\s*;?$";

        assertTrue(alterQuery.matches(alterRegex), 
            "ALTER TABLE User ADD PRIMARY KEY syntax must be present and valid in queries.sql");
    }

    // -------------------- UTIL METHODS --------------------

    private void assertTableExists(String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet tables = meta.getTables(null, "ltimindtree_db", tableName, new String[]{"TABLE"})) {
            assertTrue(tables.next(), "Table " + tableName + " should exist");
        }
    }

    private void assertTableColumns(String tableName, Map<String, String> expectedCols) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        Map<String, String> actualCols = new HashMap<>();

        try (ResultSet cols = meta.getColumns(null, "ltimindtree_db", tableName, null)) {
            while (cols.next()) {
                String colName = cols.getString("COLUMN_NAME").toLowerCase();
                String typeName = cols.getString("TYPE_NAME").toUpperCase();
                actualCols.put(colName, typeName);
            }
        }

        for (var col : expectedCols.entrySet()) {
            String colName = col.getKey().toLowerCase();
            String expectedType = col.getValue().toUpperCase();

            assertTrue(actualCols.containsKey(colName),
                "Column '" + col.getKey() + "' should exist in table " + tableName);
            String actualType = actualCols.get(colName);

            boolean matches = actualType.startsWith(expectedType)
                    || (expectedType.equals("FLOAT") && (actualType.equals("REAL") || actualType.equals("FLOAT")))
                    || (expectedType.equals("CHAR") && actualType.startsWith("CHAR"));

            assertTrue(matches,
                "Column '" + col.getKey() + "' in table " + tableName + " expected type " + expectedType + " but got " + actualType);
        }
    }
 

}
