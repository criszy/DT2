package DT2.common.oracle.txndiff;

import DT2.*;

import java.sql.*;
import java.util.*;

public class DiffTool {

    static int colCount;
    static int rowCount;
    public static SQLConnection conn;
    public static Map<String, ArrayList<SQLConnection>> connections;
    public static String databaseName = "test";
    public static ArrayList<IsolationLevel> isolationLevels;
    public static MainOptions options;
    public static List<String> tableNames;
    public static boolean testMySQL;
    public static boolean testTiDB;
    public static boolean testMariaDB;
    public static int txnNum;

    public static final BugReport bugReport = new BugReport();
    public static final int TxnSizeMin = 1;
    public static final int TxnSizeMax = 5;

    public static void initialize(MainOptions options) {
        DiffTool.options = options;
        conn = getConnectionFromOptions(options.getPort());
        connections = new HashMap<>();
        isolationLevels = new ArrayList<>();
        testMySQL = options.isTestMySQL();
        testTiDB = options.isTestTiDB();
        testMariaDB = options.isTestMariaDB();
        txnNum = 2 + new Random().nextInt(options.getTxnNum() - 1);
    }

    public static ArrayList<SQLConnection> genConnections(int port) {
        ArrayList<SQLConnection> cons = new ArrayList<>();
        int num = txnNum;
        if (options.isSetCase()) {
            num = options.getTxnNum();
        }
        for (int i = 0; i < num; i++) {
            cons.add(getConnectionFromOptions(port));
        }
        return cons;
    }

    public static void genConnection() {
        connections.put(options.getDbmsName(), genConnections(options.getPort()));
        if (testMySQL) {
            connections.put("mysql", genConnections(options.getMysqlPort()));
        }
        if (testTiDB) {
            connections.put("tidb", genConnections(options.getTidbPort()));
        }
        if (testMariaDB) {
            connections.put("mariadb", genConnections(options.getMariadbPort()));
        }
    }

    static void setIsolationLevel(SQLConnection con, IsolationLevel isolationLevel) {
        String sql = "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.getName();
        DiffTool.executeWithConn(con, sql);
    }

    public static void setAllIsolationLevels() {
        isolationLevels = new ArrayList<>(
                Arrays.asList(IsolationLevel.READ_UNCOMMITTED, IsolationLevel.READ_COMMITTED, IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE));
    }

    public static void setTableNames() {
        tableNames = new ArrayList<>();
        DiffTool.executeQueryWithCallback("SHOW TABLES", rs -> {
            try {
                while (rs.next()) {
                    String tName = rs.getString(1);
                    tableNames.add(tName);
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException("Show tables failed: ", e);
            }
        });
    }

    static SQLConnection getConnectionFromOptions(int port) {
        Connection con;
        try {
            String url = String.format("jdbc:%s://%s:%d/%s", "mysql", options.getHost(),
                    port, options.getDbName());
            con = DriverManager.getConnection(url, options.getUserName(), options.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to database: ", e);
        }
        return new SQLConnection(con);
    }

    public static boolean reproduceDBMS() {
        if (testMySQL) {
//            System.out.println("MySQL reproduce");
            if (!reproduceTables(connections.get("mysql").get(0))) {
                return false;
            }
        }
        if (testTiDB) {
//            System.out.println("TiDB reproduce");
            if (!reproduceTables(connections.get("tidb").get(0))) {
                return false;
            }
        }
        if (testMariaDB) {
//            System.out.println("MariaDB reproduce");
            if (!reproduceTables(connections.get("mariadb").get(0))) {
                return false;
            }
        }
        return true;
    }


    public static boolean reproduceTables(SQLConnection con) {
        boolean isFailed = false;
        try {
            ResultSet rs = con.createStatement().executeQuery("SHOW TABLES");
            while (rs.next()) {
                String tableName = rs.getString(1);
                if (!executeWithConn(con, "DROP TABLE IF EXISTS " + tableName)) {
                    isFailed = true;
                    break;
                }
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println(" -- drop table SQL exception: " + e.getMessage());
        }
        if (isFailed) {
            return false;
        }
        for (String sql : ReproduceState.getStatements()) {
            for (int i = 0; i < 5; i++) {
                if (executeWithConn(con, sql)) {
                    break;
                }
            }
        }
        return true;
    }

    public static void prepareTableFromScanner(Scanner input) {
        for (Map.Entry<String, ArrayList<SQLConnection>> connection : connections.entrySet()) {
            try {
                ResultSet rs = connection.getValue().get(0).createStatement().executeQuery("SHOW TABLES");
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    if (!executeWithConn(connection.getValue().get(0), "DROP TABLE IF EXISTS " + tableName)) {
                        break;
                    }
                    System.out.println(" -- drop table " + tableName);
                }
                rs.close();
            } catch (SQLException e) {
                System.out.println(" -- drop table SQL exception: " + e.getMessage());
            }
        }

        String sql;
        do {
            sql = input.nextLine();
            if (sql.equals("")) break;
            for (Map.Entry<String, ArrayList<SQLConnection>> connection : connections.entrySet()) {
                executeWithConn(connection.getValue().get(0), sql);
            }
            ReproduceState.logStatement(sql);
        } while (true);
    }

    public static Transaction readTransactionFromScanner(Scanner input, int txnId) {
        Transaction txn = new Transaction(txnId);
        String sql;
        int cnt = 0;
        do {
            if (!input.hasNext()) break;
            sql = input.nextLine();
            if (sql.equals("") || sql.equals("END")) break;
            txn.statements.add(new StatementCell(txn, cnt++, sql));
        } while (true);
        return txn;
    }

    public static String readScheduleFromScanner(Scanner input) {
        do {
            if (!input.hasNext()) break;
            String scheduleStr = input.nextLine();
            if (scheduleStr.equals("")) continue;
            if (scheduleStr.equals("END")) break;
            return scheduleStr;
        } while (true);
        return "";
    }


    public static String initialTables() {
        StringBuilder sb = new StringBuilder();
        for (String tName : tableNames) {
            sb.append(tName);
            sb.append(" {\n");
            String query = "SELECT * FROM " + tName;
            DiffTool.executeQueryWithCallback(query, rs -> {
                try {
                    rowCount = 0;
                    ResultSetMetaData metaData = rs.getMetaData();
                    colCount = metaData.getColumnCount();
                    while (rs.next()) {
                        rowCount++;
                        Object[] data = new Object[colCount];
                        for (int i = 1; i <= colCount; i++) {
                            data[i - 1] = rs.getObject(i);
                        }
                        sb.append("\t");
                        sb.append(rowCount).append(":");
                        sb.append(Arrays.toString(data)).append("\n");
                    }
                    sb.append("}\n");
                    rs.close();
                } catch (SQLException e) {
                    throw new RuntimeException("Table to view failed: ", e);
                }
            });
        }
        return sb.toString();
    }


    static void executeQueryWithCallback(String query, ResultSetHandler handler) {
        Statement statement;
        ResultSet resultSet;
        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(query);
            handler.handle(resultSet);
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            System.out.println("Execute query failed: " + query);
            e.printStackTrace();
        }
    }

    public static boolean executeWithConn(SQLConnection conn, String sql) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.execute(sql);
            statement.close();
        } catch (SQLException e) {
            System.out.println("Execute SQL failed: " + sql);
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }


    static HashMap<String, Pair<ArrayList<Object>, Integer>> getFinalStateAsList(SQLConnection con) {
        HashMap<String, Pair<ArrayList<Object>, Integer>> finaStates = new HashMap<>();
        for (String tName : tableNames) {
            finaStates.put(tName, getQueryResultAsList(con, "SELECT * FROM " + tName));
        }
        return finaStates;
    }

    static Pair<ArrayList<Object>, Integer> getQueryResultAsList(SQLConnection con, String query) {
        ArrayList<Object> res = new ArrayList<>();
        int columns = 0;
        try {
            ResultSet rs = con.createStatement().executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            columns = metaData.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    Object cell = rs.getObject(i);
                    if (cell instanceof byte[]) {
                        cell = byteArrToHexStr((byte[]) cell);
                    }
                    res.add(cell);
                }
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println(" -- get query result SQL exception: " + e.getMessage());
        }
        return new Pair<ArrayList<Object>, Integer>(res, columns);
    }

    static String byteArrToHexStr(byte[] bytes) {
        if (bytes.length == 0) {
            return "0";
        }
        final String HEX = "0123456789ABCDEF";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        sb.append("0x");
        for (byte b : bytes) {
            sb.append(HEX.charAt((b >> 4) & 0x0F));
            sb.append(HEX.charAt(b & 0x0F));
        }
        return sb.toString();
    }

}
