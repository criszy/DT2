package sqlancer.common.oracle.txndiff;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Data
public class TxnPairResult {
    IsolationLevel isolationLevel1;
    IsolationLevel isolationLevel2;
    private ArrayList<StatementCell> order;
    private HashMap<String, Pair<ArrayList<Object>, Integer>> finalState;
    private boolean deadlock;

    public TxnPairResult(IsolationLevel isolationLevel1, IsolationLevel isolationLevel2) {
        this.isolationLevel1 = isolationLevel1;
        this.isolationLevel2 = isolationLevel2;
        this.order = new ArrayList<>();
        this.finalState = null;
        this.deadlock = false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Result:\n");
        sb.append("Order:").append(order).append("\n");
        sb.append("Query Results:\n");
        for (StatementCell stmt : order) {
            sb.append("\t").append(stmt.txn.txnId).append("-").append(stmt.statementId)
                    .append(": ").append(stmt.getResult()).append("\n");
        }
        sb.append("FinalState: ").append(finalStateToString()).append("\n");
        sb.append("DeadBlock: ").append(deadlock).append("\n");
        return sb.toString();
    }

    public void setFinalState(HashMap<String, Pair<ArrayList<Object>, Integer>> finalState) {
        this.finalState = finalState;
    }

    public boolean isDeadlock() {
        return deadlock;
    }

    public void setDeadlock(boolean deadlock) {
        this.deadlock = deadlock;
    }

    public void setOrder(ArrayList<StatementCell> order) {
        this.order = order;
    }

    public ArrayList<StatementCell> getOrder() {
        return order;
    }

    public HashMap<String, Pair<ArrayList<Object>, Integer>> getFinalState() {
        return finalState;
    }

    public String finalStateToString() {
        StringBuilder sb = new StringBuilder("[");
        for (Map.Entry<String, Pair<ArrayList<Object>, Integer>> entry : finalState.entrySet()) {
            sb.append(entry.getKey()).append(": [");
            for (Object rs : entry.getValue().getLeft()) {
                sb.append(rs);
                sb.append(", ");
            }
            if (sb.lastIndexOf(",") == sb.length() - 2) {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append("] ");
        }
        sb.append("]");
        return sb.toString();
    }
}
