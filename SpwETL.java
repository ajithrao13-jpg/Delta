import java.sql.*;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class SpwETL {

    private static final String URL =
        "jdbc:sqlserver://localhost:1433;databaseName=Nandu;encrypt=false;trustServerCertificate=true;integratedSecurity=true";

    public static void main(String[] args) {

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            try (Connection conn = DriverManager.getConnection(URL)) {

                conn.setAutoCommit(false);

                // ✅ 0️⃣ TRUNCATE STAGE TABLES
                truncateStageTables(conn);

                // ✅ 1️⃣ Load RAW → STAGE
                loadAccounts(conn);
                loadPositions(conn);

                // ✅ 2️⃣ Run Delta Merge Stored Procedures
                runDeltaProcedures(conn);

                conn.commit();
                System.out.println("\n✅ ETL + Delta completed successfully!");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =========================================================
    // TRUNCATE STAGE TABLES
    // =========================================================
    private static void truncateStageTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE STG_SPW_ACCOUNT_DETAIL");
            stmt.execute("TRUNCATE TABLE STG_SPW_POSITION_DETAIL");
            System.out.println("✔ Stage tables truncated");
        }
    }

    // =========================================================
    // LOAD ACCOUNTS → STAGE
    // =========================================================
    private static void loadAccounts(Connection conn) throws Exception {

        String selectSql = "SELECT * FROM RAW_SPW_ACCOUNT_DETAIL";
        String insertSql =
            "INSERT INTO STG_SPW_ACCOUNT_DETAIL " +
            "(ID, PACKAGE_ID, account_number, open_date, market_value, is_pledged, SYSTEM_DATETIME, row_hash) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
             ResultSet rs = selectStmt.executeQuery();
             PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {

            int batchSize = 0;

            while (rs.next()) {

                long id = rs.getLong("ID");
                long packageId = rs.getLong("PACKAGE_ID");

                String accountNumber = trim(rs.getString("account_number"));
                LocalDate openDate = parseDate(rs.getString("open_date"));
                BigDecimal marketValue = parseDecimal(rs.getString("market_value"));
                boolean isPledged = "Y".equalsIgnoreCase(rs.getString("pledge_indicator"));
                Timestamp systemDateTime = rs.getTimestamp("SYSTEM_DATETIME");

                byte[] hash = generateHash(
                        accountNumber,
                        rs.getString("open_date"),
                        rs.getString("market_value"),
                        rs.getString("pledge_indicator"),
                        String.valueOf(packageId)
                );

                insertStmt.setLong(1, id);
                insertStmt.setLong(2, packageId);
                insertStmt.setString(3, accountNumber);
                insertStmt.setObject(4, openDate);
                insertStmt.setBigDecimal(5, marketValue);
                insertStmt.setBoolean(6, isPledged);
                insertStmt.setTimestamp(7, systemDateTime);
                insertStmt.setBytes(8, hash);

                insertStmt.addBatch();
                batchSize++;

                if (batchSize % 500 == 0) {
                    insertStmt.executeBatch();
                }
            }

            insertStmt.executeBatch();
            System.out.println("✔ Accounts loaded to STAGE");
        }
    }

    // =========================================================
    // LOAD POSITIONS → STAGE
    // =========================================================
    private static void loadPositions(Connection conn) throws Exception {

        String selectSql = "SELECT * FROM RAW_SPW_POSITION_DETAIL";
        String insertSql =
            "INSERT INTO STG_SPW_POSITION_DETAIL " +
            "(ID, PACKAGE_ID, account_number, cusip, symbol, market_price, quantity, market_value, valuation_date, SYSTEM_DATETIME, row_hash) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
             ResultSet rs = selectStmt.executeQuery();
             PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {

            int batchSize = 0;

            while (rs.next()) {

                long id = rs.getLong("ID");
                long packageId = rs.getLong("PACKAGE_ID");

                String accountNumber = trim(rs.getString("account_number"));
                String cusip = rs.getString("cusip");
                String symbol = rs.getString("symbol");

                BigDecimal marketPrice = parseDecimal(rs.getString("market_price"));
                BigDecimal quantity = parseDecimal(rs.getString("quantity"));
                BigDecimal marketValue = parseDecimal(rs.getString("market_value"));

                LocalDate valuationDate = parseDate(rs.getString("date_last_valuation"));
                Timestamp systemDateTime = rs.getTimestamp("SYSTEM_DATETIME");

                byte[] hash = generateHash(
                        accountNumber, cusip, symbol,
                        rs.getString("market_price"),
                        rs.getString("quantity"),
                        rs.getString("market_value"),
                        rs.getString("date_last_valuation"),
                        String.valueOf(packageId)
                );

                insertStmt.setLong(1, id);
                insertStmt.setLong(2, packageId);
                insertStmt.setString(3, accountNumber);
                insertStmt.setString(4, cusip);
                insertStmt.setString(5, symbol);
                insertStmt.setBigDecimal(6, marketPrice);
                insertStmt.setBigDecimal(7, quantity);
                insertStmt.setBigDecimal(8, marketValue);
                insertStmt.setObject(9, valuationDate);
                insertStmt.setTimestamp(10, systemDateTime);
                insertStmt.setBytes(11, hash);

                insertStmt.addBatch();
                batchSize++;

                if (batchSize % 500 == 0) {
                    insertStmt.executeBatch();
                }
            }

            insertStmt.executeBatch();
            System.out.println("✔ Positions loaded to STAGE");
        }
    }

    // =========================================================
    // RUN DELTA STORED PROCEDURES
    // =========================================================
    private static void runDeltaProcedures(Connection conn) throws SQLException {

        System.out.println("\n▶ Running delta stored procedures...");

        try (CallableStatement acctStmt =
                     conn.prepareCall("{call dbo.SP_DELTA_SPW_ACCOUNT_DETAIL}");
             CallableStatement posStmt  =
                     conn.prepareCall("{call dbo.SP_DELTA_SPW_POSITION_DETAIL}")) {

            acctStmt.execute();
            System.out.println("✔ Account delta merge complete");

            posStmt.execute();
            System.out.println("✔ Position delta merge complete");
        }
    }

    // =========================================================
    // HELPERS
    // =========================================================
    private static String trim(String value) {
        return value == null ? null : value.trim();
    }

    private static BigDecimal parseDecimal(String value) {
        if (value == null || value.isBlank()) return null;
        return new BigDecimal(value.replace(",", ""));
    }

    private static LocalDate parseDate(String value) {
        if (value == null || value.isBlank()) return null;

        if (value.contains("-"))
            return LocalDate.parse(value);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return LocalDate.parse(value, formatter);
    }

    private static byte[] generateHash(String... fields) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        StringBuilder sb = new StringBuilder();
        for (String field : fields) {
            sb.append(field == null ? "" : field).append("|");
        }
        return digest.digest(sb.toString().getBytes());
    }
}