package sqlancer.common.oracle.txndiff;

import sqlancer.SQLConnection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public class TxnPairExecutor {
    private final ArrayList<StatementCell> submittedOrder;

    private SQLConnection con1;
    private SQLConnection con2;
    private ArrayList<SQLConnection> cons;
    private TxnPairResult result;
    private String exceptionMessage = "";

    public TxnPairExecutor(ArrayList<StatementCell> schedule, IsolationLevel isolationLevel, ArrayList<SQLConnection> cons) {
        this.submittedOrder = schedule;
        this.cons = cons;
        for (int i = 0; i < cons.size(); i++) {
            DiffTool.setIsolationLevel(cons.get(i), isolationLevel);
        }
        this.result = new TxnPairResult(isolationLevel, isolationLevel);
    }

    TxnPairResult execute() {
        HashMap<Integer, BlockingQueue<StatementCell>> queues = new HashMap<>();
        HashMap<Integer, BlockingQueue<StatementCell>> communications = new HashMap<>();
        for (int i = 1; i <= cons.size(); i++) {
            BlockingQueue<StatementCell> queue = new SynchronousQueue<>();
            BlockingQueue<StatementCell> communication = new SynchronousQueue<>();
            queues.put(i, queue);
            communications.put(i, communication);
            Thread consumer = new Thread(new Consumer(queue, communication, cons.get(i - 1)));
            consumer.start();
        }
        Thread producer = new Thread(new Producer(queues, communications, submittedOrder));
        producer.start();
        while (producer.isAlive()) {
        }
        // do not ignore deadlock
        result.setFinalState(DiffTool.getFinalStateAsList(cons.get(1)));
        return result;
    }

    class Producer implements Runnable {
        private ArrayList<StatementCell> schedule;
        private HashMap<Integer, BlockingQueue<StatementCell>> queues;
        private HashMap<Integer, BlockingQueue<StatementCell>> communications;

        public Producer(HashMap<Integer, BlockingQueue<StatementCell>> queues,
                        HashMap<Integer, BlockingQueue<StatementCell>> communications,
                        ArrayList<StatementCell> schedule) {
            this.queues = queues;
            this.communications = communications;
            this.schedule = schedule;
        }

        public void run() {
            Map<Integer, Boolean> txnBlock = new HashMap<>(); // whether a child thread is blocked
            for (int i = 1; i <= queues.size(); i++) {
                txnBlock.put(i, false);
            }
            clearSubmitted();
            Long startTs = System.currentTimeMillis(); // record time threshold
            int stmtID; // statement ID
            ArrayList<StatementCell> actualSchedule = new ArrayList<>();
            while (!isAllStmtsSubmitted()) {
                for (stmtID = 0; stmtID < schedule.size(); stmtID++) {
                    if (schedule.get(stmtID).submitted) {
                        continue;
                    }
                    int txn = schedule.get(stmtID).txn.txnId;
                    StatementCell statementCell = schedule.get(stmtID).copy();
                    if (txnBlock.get(txn)) { // if a child thread is blocked
                        continue;
                    }
                    try {
                        queues.get(txn).put(statementCell);
                    } catch (InterruptedException e) {
                        System.out.println(" -- MainThread run exception");
                        System.out.println("Statement: " + statementCell.statement);
                        System.out.println("Interrupted Exception: " + e.getMessage());
                    }
                    schedule.get(stmtID).submitted = true;
                    StatementCell stmtReturn = communications.get(txn).poll(); // communicate with a child thread
                    startTs = System.currentTimeMillis();
                    while (stmtReturn == null) { // wait for 2s
                        if (System.currentTimeMillis() - startTs > 2000) { // child thread is blocked
                            System.out.println(" -- " + txn + "-" + statementCell.statementId + ": time out 1");
                            txnBlock.put(txn, true); // record blocked transaction
                            StatementCell blockPoint = statementCell.copy();
                            blockPoint.blocked = true;
                            actualSchedule.add(blockPoint);
                            break;
                        }
                        stmtReturn = communications.get(txn).poll();
                    }
                    if (stmtReturn == null) {
                        continue;
                    } else { // success to receive feedback
                        if (stmtReturn.exceptionMessage.length() > 0) {
                            stmtReturn.error = true;
                            if (stmtReturn.exceptionMessage.contains("Deadlock") || stmtReturn.exceptionMessage.contains("lock=true")) {
                                result.setDeadlock(true);
                            }
                            stmtReturn.exceptionMessage = "";
                        } else if (stmtReturn.warnings.getLeft().size() > 0 && !stmtReturn.statement.contains("BEGIN") && !stmtReturn.statement.contains("COMMIT") && !stmtReturn.statement.contains("ROLLBACK")) {
                            stmtReturn.warning = true;
                        }
                        actualSchedule.add(stmtReturn);
                        boolean reSumed = false;
                        for (int i = 1; i <= communications.size(); i++) {
                            if (i == txn) {
                                continue;
                            }
                            StatementCell reSumedStmt = communications.get(i).poll();
                            while (reSumedStmt == null) { // wait for 2s
                                if (System.currentTimeMillis() - startTs > 2000) { // child thread is blocked
                                    break;
                                }
                                reSumedStmt = communications.get(i).poll();
                            }
                            if (reSumedStmt != null) {
                                reSumed = true;
                                txnBlock.replace(reSumedStmt.txn.txnId, false);
                                if (reSumedStmt.exceptionMessage.length() > 0) {
                                    reSumedStmt.error = true;
                                    if (reSumedStmt.exceptionMessage.contains("Deadlock") || reSumedStmt.exceptionMessage.contains("lock=true")) {
                                        result.setDeadlock(true);
                                    }
                                    reSumedStmt.exceptionMessage = "";
                                } else if (reSumedStmt.warnings.getLeft().size() > 0) {
                                    reSumedStmt.warning = true;
                                }
                                actualSchedule.add(reSumedStmt);
                            }
                        }
                        if (reSumed) {
                            break;
                        }
                    }
                }
            }
            Transaction txn0 = new Transaction(0);
            StatementCell stopThread = new StatementCell(txn0);
            try {
                for (int i = 1; i <= queues.size(); i++) {
                    queues.get(i).put(stopThread);
                }
            } catch (InterruptedException e) {
                System.out.println(" -- MainThread stop child thread Interrupted exception: " + e.getMessage());
            }
            result.setOrder(actualSchedule);
        }

        public boolean isAllStmtsSubmitted() {
            for (int i = 0; i < schedule.size(); i++) {
                if (!schedule.get(i).submitted) {
                    return false;
                }
            }
            return true;
        }

        public void clearSubmitted() {
            for (int i = 0; i < schedule.size(); i++) {
                schedule.get(i).submitted = false;
            }
        }

    }

    class Consumer implements Runnable {
        private SQLConnection conn;
        private BlockingQueue<StatementCell> queue;
        private final BlockingQueue<StatementCell> communication; // represent the execution feedback

        public Consumer(BlockingQueue<StatementCell> queue, BlockingQueue<StatementCell> communicationID, SQLConnection conn) {
            this.queue = queue;
            this.communication = communicationID;
            this.conn = conn;
        }

        public void run() {
            try {
                while (true) {
                    StatementCell statementCell = queue.take(); // communicate with main thread
                    if (statementCell.txn.txnId > 0) { // stop condition: schedule.size()
                        threadExec(statementCell);
                    } else {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                // thread stop
                System.out.println(" -- TXNThread run Interrupted exception: " + e.getMessage());
            }
        }

        // execute a query
        void threadExec(StatementCell stmt) {
            String query = stmt.statement;
            String exceptionMessage = "";
            try {
                if (stmt.type == StatementType.BEGIN) {
                    DiffTool.executeWithConn(conn, "BEGIN");
                } else if (stmt.type == StatementType.SELECT || stmt.type == StatementType.SELECT_UPDATE || stmt.type == StatementType.SELECT_SHARE) {
                    stmt.result = getQueryResultAsList(conn, query);
                } else {
                    conn.createStatement().executeUpdate(query);
                }
                stmt.warnings = getQueryResultAsList(conn, "SHOW WARNINGS");
            } catch (SQLException e) { // connection.createStatement().executeQuery()
                System.out.println(" -- TXNThread threadExec exception");
                System.out.println("Statement " + stmt + ": " + query);
                exceptionMessage = e.getMessage();
                System.out.println("SQL Exception: " + exceptionMessage);
                stmt.exceptionMessage = stmt + ": " + exceptionMessage;
            } finally {
                try {
                    communication.put(stmt); // communicate to main thread
                } catch (InterruptedException e) { // communicationID.put()
                    System.out.println(" -- TXNThread threadExec exception");
                    System.out.println("Query " + stmt + ": " + query);
                    System.out.println("Interrupted Exception: " + e.getMessage());
                }
            }
        }

        Pair<ArrayList<Object>, Integer> getQueryResultAsList(SQLConnection con, String query) {
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
                System.out.println(" -- TXNThread threadExec exception");
                System.out.println("Query: " + query);
                exceptionMessage = e.getMessage();
                System.out.println("SQL Exception: " + exceptionMessage);
                exceptionMessage = exceptionMessage + "; [Query] " + query;
            }
            return new Pair<ArrayList<Object>, Integer>(res, columns);
        }

        String byteArrToHexStr(byte[] bytes) {
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
}
