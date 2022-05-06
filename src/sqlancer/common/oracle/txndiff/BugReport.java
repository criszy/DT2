package sqlancer.common.oracle.txndiff;

import java.util.HashMap;
import java.util.List;

public class BugReport {
    private String bugInfo;
    private List<String> initializeStatements;
    private String initialTable;
    private String inputSchedule;
    private Pair<String, TxnPairResult> result1;
    private Pair<String, TxnPairResult> result2;
    private String submittedOrder;
    private HashMap<Integer, Transaction> txns;

    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        sb.append("============================= TRANSACTION BUG REPORT =============================\n");
        for (String stmt : initializeStatements) {
            sb.append(stmt).append(";\n");
        }
        sb.append(" -- Initial Table: \n").append(initialTable).append("\n");
        for (Transaction txn : txns.values()) {
            sb.append(txn).append("\n");
        }
        sb.append(" -- Input Schedule: ").append(inputSchedule).append("\n");
        sb.append(" -- Submitted Order: ").append(submittedOrder).append("\n");
        sb.append(" -- Isolation Level: ").append(result1.getRight().isolationLevel1.getName()).append("\n");
        sb.append(" -- " + result1.getLeft() + ": ").append(result1.getRight()).append("\n");
        sb.append(" -- " + result2.getLeft() + ": ").append(result2.getRight()).append("\n");
        sb.append(bugInfo).append("\n");
        return sb.toString();
    }

    public void setBugInfo(String bugInfo) {
        this.bugInfo = bugInfo;
    }

    public void setInitializeStatements(List<String> initializeStatements) {
        this.initializeStatements = initializeStatements;
    }

    public void setInitialTable(String initialTable) {
        this.initialTable = initialTable;
    }

    public void setTxns(HashMap<Integer, Transaction> txns) {
        this.txns = txns;
    }

    public void setInputSchedule(String inputSchedule) {
        this.inputSchedule = inputSchedule;
    }

    public void setSubmittedOrder(String submittedOrder) {
        this.submittedOrder = submittedOrder;
    }

    public void setResult1(Pair<String, TxnPairResult> result1) {
        this.result1 = result1;
    }

    public void setResult2(Pair<String, TxnPairResult> result2) {
        this.result2 = result2;
    }
}
