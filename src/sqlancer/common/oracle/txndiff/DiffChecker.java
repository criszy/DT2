package sqlancer.common.oracle.txndiff;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.SQLConnection;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DiffChecker {
    protected HashMap<Integer, Transaction> txns;
    private String bugInfo;
    public static final int checkSize = 5;


    public DiffChecker(HashMap<Integer, Transaction> txns) {
        this.txns = txns;
    }

    public boolean checkSeveral(GlobalState state) {
        ArrayList<ArrayList<StatementCell>> submittedOrderList = ShuffleTool.genSeveralSubmittedTrace(txns, checkSize);
        int count = 0;
        boolean findBug = false;
        long startTime = System.currentTimeMillis();
        for (ArrayList<StatementCell> submittedOrder : submittedOrderList) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
            count++;
            state.getLogger().writeCurrent("Order: ");
            System.out.println("Order: " + count);
            if (!oracleCheck(submittedOrder, state)) {
                findBug = true;
                break;
            }
        }
        long endTime = System.currentTimeMillis() - startTime;
        StringBuilder sb = new StringBuilder();
        sb.append("============================= One transaction group execution statistics =============================\n");
        sb.append("submitted order number: " + count + "\n");
        sb.append("Time: " + endTime + "ms");
        state.getLogger().writeCurrent(sb.toString());
        System.out.println(sb.toString());
        return findBug;
    }


    public void checkSchedule(String scheduleStr, GlobalState state) {
        String[] schedule = scheduleStr.split("-");
        int len = 0;
        for (int i = 1; i <= txns.size(); i++) {
            len = len + txns.get(i).statements.size();
        }
        if (schedule.length != len) {
            throw new RuntimeException("Invalid Schedule");
        }
        ArrayList<StatementCell> submittedOrder = new ArrayList<>();
        HashMap<Integer, Integer> idx = new HashMap<>();
        for (int i = 1; i <= txns.size(); i++) {
            idx.put(i, 0);
        }
        for (String txnId : schedule) {
            int tid = Integer.parseInt(txnId);
            int index = idx.get(tid);
            submittedOrder.add(txns.get(tid).statements.get(index));
            index++;
            idx.replace(tid, index);
        }
        oracleCheck(submittedOrder, state);
    }

    public void checkAll(GlobalState state) {
        boolean findBug = false;
        ArrayList<ArrayList<StatementCell>> submittedOrderList = ShuffleTool.genAllSubmittedTrace(txns);
        for (ArrayList<StatementCell> submittedOrder : submittedOrderList) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
            if (!oracleCheck(submittedOrder, state)) {
                findBug = true;
                break;
            }
        }
        if (findBug) {
            System.exit(0);
        }
        state.getLogger().writeCurrent("Check all orders finish");
        System.out.println("Check all orders finish");
    }

    private boolean checkTestCase(ArrayList<StatementCell> schedule) {
        for (StatementCell stmt : schedule) {
            if (stmt.type == StatementType.SELECT_SHARE) {
                return true;
            }
        }
        return false;
    }

    private boolean oracleCheck(ArrayList<StatementCell> schedule, GlobalState state) {
        System.out.println("Check new schedule.");
        ArrayList<IsolationLevel> isolationLevels = DiffTool.isolationLevels;
        ArrayList<Pair<String, TxnPairResult>> resultList = new ArrayList<>();
        System.out.println("Input schedule: " + schedule.toString());
        boolean isSame = true;
        for (IsolationLevel isolationLevel : isolationLevels) {
            System.out.println("Isolation level: " + isolationLevel);
            for (Map.Entry<String, ArrayList<SQLConnection>> entry : DiffTool.connections.entrySet()) {
                if (checkTestCase(schedule) && entry.getKey().equals("tidb")) {
                    continue;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
                if ((isolationLevel.getName().equals("READ UNCOMMITTED") || isolationLevel.getName().equals("SERIALIZABLE")) && entry.getKey().equals("tidb")) {
                    continue;
                }
                System.out.println("execute " + entry.getKey());
                SQLConnection con1 = entry.getValue().get(0);
                if (!DiffTool.reproduceTables(con1)) {
                    System.out.println("Transaction Execution Initial Failed");
                    return false;
                }
                TxnPairExecutor executor = new TxnPairExecutor(schedule, isolationLevel, entry.getValue());
                TxnPairResult execResult = executor.execute();
                Pair<String, TxnPairResult> resultPair = new Pair<>(entry.getKey(), execResult);
                resultList.add(resultPair);
                if (DiffTool.options.isSetCase()) {
                    System.out.println("Get " + entry.getKey() + " " + execResult);
                }
            }
            if (DiffTool.options.isSetCase()) {
                for (int i = 0; i < resultList.size() - 1; i++) {
                    for (int j = i + 1; j < resultList.size(); j++) {
                        bugInfo = "";
                        if (!compareOracles(resultList.get(i).getRight(), resultList.get(j).getRight())) {
                            System.out.println(isolationLevel.getName());
                            System.out.println(resultList.get(i).getLeft() + " & " + resultList.get(j).getLeft());
                            System.out.println(bugInfo + "\n");
                        }
                    }
                }
            } else {
                for (int i = 0; i < resultList.size() - 1; i++) {
                    for (int j = i + 1; j < resultList.size(); j++) {
                        bugInfo = "";
                        if (!compareOracles(resultList.get(i).getRight(), resultList.get(j).getRight())) {
                            DiffTool.bugReport.setInputSchedule(getScheduleInputStr(schedule));
                            DiffTool.bugReport.setSubmittedOrder(schedule.toString());
                            DiffTool.bugReport.setBugInfo(bugInfo);
                            DiffTool.bugReport.setResult1(resultList.get(i));
                            DiffTool.bugReport.setResult2(resultList.get(j));
                            state.getLogger().writeCurrent(DiffTool.bugReport.toString());
                            System.out.println(DiffTool.bugReport.toString());
                            Main.bugCases++;
                            isSame = false;
                        }

                    }
                }
            }
            resultList.clear();
        }
        return isSame;
    }

    private String getScheduleInputStr(ArrayList<StatementCell> schedule) {
        ArrayList<String> order = new ArrayList<>();
        for (StatementCell stmt : schedule) {
            order.add(Integer.toString(stmt.txn.txnId));
        }
        return String.join("-", order);
    }

    private boolean compareOracles(TxnPairResult result1, TxnPairResult result2) {
        ArrayList<StatementCell> order1 = result1.getOrder();
        ArrayList<StatementCell> order2 = result2.getOrder();
        int minLen = Math.min(order1.size(), order2.size());
        boolean isSame = true;
        for (int i = 0; i < minLen; i++) {
            StatementCell stmt1 = order1.get(i);
            StatementCell stmt2 = order2.get(i);
            if (!stmt2.blocked && !stmt1.blocked) {
                if ((stmt1.error && !stmt2.error) || (!stmt1.error && stmt2.error)) {
                    if (result1.isDeadlock() || result2.isDeadlock()) {
                        bugInfo += " -- Error: Inconsistent deadlock\n";
                        bugInfo += " -- Statement: (" + stmt2.txn.txnId + "-" + stmt2.statementId + ") " + stmt2.statement;
                        isSame = false;
                        break;
                    }
                    bugInfo = " -- Error: Inconsistent error report\n";
                    bugInfo += " -- Statement: (" + stmt2.txn.txnId + "-" + stmt2.statementId + ") " + stmt2.statement + "\n";
                    isSame = false;
                    break;
                }
                if (stmt1.warning && !stmt2.warning || !stmt1.warning && stmt2.warning) {
                    bugInfo += " -- Error: Inconsistent error report\n";
                    bugInfo += " -- Statement: (" + stmt2.txn.txnId + "-" + stmt2.statementId + ") " + stmt2.statement;
                    isSame = false;
                    break;
                }
                if (stmt2.type == StatementType.SELECT || stmt2.type == StatementType.SELECT_UPDATE || stmt2.type == StatementType.SELECT_SHARE) {
                    if (!compareResultSets(stmt1.result.getLeft(), stmt2.result.getLeft(), stmt1.result.getRight())) {
                        bugInfo += " -- Error: Inconsistent query result \n";
                        bugInfo += " -- Query: (" + stmt2.txn.txnId + "-" + stmt2.statementId + ") " + stmt2.statement + "\n";
                        isSame = false;
                        break;
                    }
                }
            }
            if (stmt2.blocked && !stmt1.blocked || !stmt2.blocked && stmt1.blocked) {
                bugInfo = " -- Error: Inconsistent statement lock\n";
                bugInfo += " -- Statement: (" + stmt1.txn.txnId + "-" + stmt1.statementId + ") " + stmt1.statement + "\n";
                isSame = false;
                break;
            }
        }
        for (String tName : DiffTool.tableNames) {
            if (!compareResultSets(result1.getFinalState().get(tName).getLeft(), result2.getFinalState().get(tName).getLeft(), result1.getFinalState().get(tName).getRight())) {
                bugInfo += " -- Error: Inconsistent database final state";
                return false;
            }
        }
        return isSame;
    }


    private boolean compareResultSets(ArrayList<Object> resultSet1, ArrayList<Object> resultSet2, int columnCount) {
        if (resultSet1.size() != resultSet2.size()) {
            bugInfo += " -- Number Of Data Different\n";
            return false;
        }
        for (int i = 0; i < resultSet1.size(); i++) {
            int row = i / columnCount + 1;
            int column = i % columnCount + 1;
            Object result1 = resultSet1.get(i);
            Object result2 = resultSet2.get(i);
            if (result1 == null && result2 == null) {
                continue;
            }
            if (result1 == null || result2 == null) {
                bugInfo += " -- Result: (" + row + "." + column + ") Values Different [" + result1 + ", " + result2 + "]\n";
                return false;
            }
            if (!result1.equals(result2)) {
                if (!compareFloat(result1, result2)) {
                    bugInfo += " -- Result: (" + row + "." + column + ") Values Different [" + result1 + ", " + result2 + "]\n";
                    return false;
                }
            }
        }
        return true;
    }


    private boolean compareFloat(Object result1, Object result2) {
        if ((result1 instanceof Float) || (result1 instanceof Double)) {
            String str1 = String.valueOf(result1);
            String str2 = String.valueOf(result2);
            int len = str1.indexOf(".");
            int lens = Math.min(str1.length(), str2.length());
            if (len != -1) {
                len = lens - len - 1;
                if (str1.length() <= str2.length()) {
                    BigDecimal bd = new BigDecimal(str2);
                    if (result1 instanceof Float) {
                        float temp = bd.setScale(len, RoundingMode.HALF_UP).floatValue();
                        if (temp == (Float) result1) {
                            return true;
                        }
                    } else {
                        double temp = bd.setScale(len, RoundingMode.HALF_UP).doubleValue();
                        if (temp == (Double) result1) {
                            return true;
                        }
                    }
                } else {
                    BigDecimal bd = new BigDecimal(str1);
                    if (result1 instanceof Float) {
                        float temp = bd.setScale(len, RoundingMode.HALF_UP).floatValue();
                        if (temp == (Float) result2) {
                            return true;
                        }
                    } else {
                        double temp = bd.setScale(len, RoundingMode.HALF_UP).doubleValue();
                        if (temp == (Double) result2) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
