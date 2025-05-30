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
        // Connect to MySQL database ltimindtree_db (adjust user/password as needed)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/ltimindtree_db", "root", "");
        
        // Removed reading and executing queries.sql here
        // Assumed schema & data are already created in DB before tests run
    }

    @AfterAll
    public void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    @DisplayName("Check if User table has primary key on id column")
    public void testUserTablePrimaryKeyExists() throws SQLException {
        String checkPKQuery =
            "SELECT COUNT(*) AS pk_exists " +
            "FROM information_schema.table_constraints " +
            "WHERE table_schema = 'ltimindtree_db' " +
            "AND table_name = 'User' " +
            "AND constraint_type = 'PRIMARY KEY'";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(checkPKQuery)) {

            assertTrue(rs.next(), "ResultSet should have at least one row.");
            int pkExists = rs.getInt("pk_exists");
            assertEquals(1, pkExists, "User table must have a PRIMARY KEY constraint defined.");
        }
    }
    @Test
    public void testDatabaseCreated() throws SQLException {
        String expectedSchema = "ltimindtree_db";
        
        String query = "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, expectedSchema);
            try (ResultSet rs = stmt.executeQuery()) {
                boolean schemaExists = rs.next();
                assertTrue(schemaExists, "Schema '" + expectedSchema + "' should exist");
            }
        }
    }


    @Test
    public void testTablesCreatedWithCorrectColumns() throws SQLException {
        String schema = "ltimindtree_db";

        Map<String, Map<String, String>> expectedTables = new HashMap<>();

        expectedTables.put("Admin", Map.of(
            "id", "VARCHAR",
            "admin_name", "VARCHAR",
            "email_id", "VARCHAR",
            "password", "VARCHAR"
        ));

        expectedTables.put("User", Map.of(
            "id", "VARCHAR",
            "user_name", "VARCHAR",
            "email_id", "VARCHAR",
            "password", "VARCHAR",
            "gender", "CHAR",
            "city", "VARCHAR",
            "mobile_number", "CHAR",
            "zipcode", "CHAR"
        ));

        expectedTables.put("Product", Map.of(
            "id", "VARCHAR",
            "product_name", "VARCHAR",
            "category", "VARCHAR",
            "price", "FLOAT",
            "quantity", "SMALLINT",
            "offers", "VARCHAR",
            "description", "VARCHAR"
        ));

        expectedTables.put("Payment_method", Map.of(
            "id", "VARCHAR",
            "account_holder_name", "VARCHAR",
            "account_number", "CHAR",
            "date_of_payment", "DATE"
        ));

        DatabaseMetaData meta = conn.getMetaData();

        for (var entry : expectedTables.entrySet()) {
            String tableName = entry.getKey();
            Map<String, String> expectedCols = entry.getValue();

            try (ResultSet tables = meta.getTables(null, schema, tableName, new String[]{"TABLE"})) {
                assertTrue(tables.next(), "Table " + tableName + " should exist");
            }

            Map<String, String> actualCols = new HashMap<>();
            try (ResultSet cols = meta.getColumns(null, schema, tableName, null)) {
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

    @Test
    public void testPrimaryKeyOnUser() throws SQLException {
        String schema = "ltimindtree_db";
        String table = "User";

        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet pk = meta.getPrimaryKeys(null, schema, table)) {
            boolean pkOnId = false;
            while (pk.next()) {
                String colName = pk.getString("COLUMN_NAME");
                if ("id".equalsIgnoreCase(colName)) {
                    pkOnId = true;
                    break;
                }
            }
            assertTrue(pkOnId, "Primary key should be set on 'id' column of User table");
        }
    }

    // This test is now hardcoded with the expected SELECT query instead of reading from queries.sql
//    @Test
//    public void testSelectProductsUnder2000Electronics() throws SQLException {
//        String learnerQuery = "SELECT product_name FROM Product WHERE price < 2000 AND category = 'electronics'";
//
//        try (PreparedStatement stmt = conn.prepareStatement(learnerQuery);
//             ResultSet rs = stmt.executeQuery()) {
//
//            Set<String> expectedProducts = Set.of("EarPhones");
//            Set<String> actualProducts = new HashSet<>();
//
//            while (rs.next()) {
//                actualProducts.add(rs.getString("product_name"));
//            }
//
//            assertEquals(expectedProducts, actualProducts,
//                "The output of the SELECT query is incorrect. Expected product(s) with price < 2000 and category 'electronics'.");
//        }
//    }

    @Test
    @Order(1)
    public void testUserTableInsert() throws SQLException {
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
    }@Test
    public void testInsertIntoProductTable() throws SQLException {
        // Verify the number of expected records
        String countQuery = "SELECT COUNT(*) AS total FROM Product";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            assertTrue(rs.next(), "ResultSet should have at least one row");
            int total = rs.getInt("total");
            assertEquals(5, total, "Product table should contain 5 records");
        }

        // Validate each record individually
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

    @Test
    public void testLearnerQueryOutputMatchesExpectedIgnoreCase() throws Exception {
        // Read learner's query from resources/queries.sql using classloader
        InputStream is = getClass().getClassLoader().getResourceAsStream("queries.sql");
        assertNotNull(is, "queries.sql file not found in resources folder");

        String learnerQuery;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            learnerQuery = reader.lines().collect(Collectors.joining("\n")).trim();
        }

        List<String> learnerResults = new ArrayList<>();
        List<String> expectedResults = Arrays.asList("EarPhones");

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(learnerQuery)) {
            while (rs.next()) {
                learnerResults.add(rs.getString("product_name").toLowerCase());
            }
        }

  List<String> expectedLower = expectedResults.stream()
                                .map(String::toLowerCase)
                                .sorted()
                                .collect(Collectors.toList());


        assertEquals(expectedLower, learnerResults, "Learner output should match expected result ignoring case.");
    }

    
}
