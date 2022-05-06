package sqlancer.common.oracle.txndiff;

import java.util.ArrayList;

enum StatementType {
    UNKNOWN,
    SELECT, SELECT_SHARE, SELECT_UPDATE,
    UPDATE, DELETE, INSERT, SET,
    BEGIN, COMMIT, ROLLBACK,
}

public class StatementCell {
    Transaction txn;
    int statementId;
    String statement;
    StatementType type;
    boolean blocked;
    boolean error;
    boolean warning;
    boolean submitted;
    String exceptionMessage;
    Pair<ArrayList<Object>, Integer> warnings;
    Pair<ArrayList<Object>, Integer> result;

    StatementCell(Transaction txn) {
        this.txn = txn;
    }

    StatementCell(Transaction txn, int statementId) {
        this.txn = txn;
        this.statementId = statementId;
        this.submitted = false;
    }

    public StatementCell(Transaction txn, int statementId, String statement) {
        this.txn = txn;
        this.statementId = statementId;
        this.statement = statement.replace(";", "");
        this.type = StatementType.valueOf(this.statement.split(" ")[0]);
        this.submitted = false;
        setSelectType();
    }

    public void setSelectType() {
        int forIdx = -1;
        StatementType realType = type;
        String stmt = this.statement;
        switch (type) {
            case SELECT:
                forIdx = stmt.indexOf("FOR ");
                if (forIdx != -1) {
                    String postfix = stmt.substring(forIdx);
                    if (postfix.equals("FOR UPDATE")) {
                        realType = StatementType.SELECT_UPDATE;
                    } else if (postfix.equals("FOR SHARE")) {
                        realType = StatementType.SELECT_SHARE;
                    } else {
                        throw new RuntimeException("Invalid postfix: " + this.statement);
                    }
                }
                this.type = realType;
                break;
            default:
                break;
        }
    }


    public String toString() {
        String res = txn.txnId + "-" + statementId;
        if (blocked) {
            res += "(B)";
        }
        if (warning) {
            res += "(W)";
        }
        if (error) {
            res += "(E)";
        }
        return res;
    }

    public String getResult() {
        if (type.equals(StatementType.SELECT) || type.equals(StatementType.SELECT_UPDATE) || type.equals(StatementType.SELECT_SHARE)) {
            if (result == null) {
                return null;
            }
            if (result.getLeft().isEmpty()) {
                return "[ ]";
            }
            StringBuilder sb = new StringBuilder("[");
            for (Object rs : result.getLeft()) {
                sb.append(rs);
                sb.append(", ");
            }
            if (sb.indexOf(",") != -1) {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append("]");
            return sb.toString();
        } else {
            return null;
        }
    }


    public StatementCell copy() {
        StatementCell copy = new StatementCell(txn, statementId);
        copy.statement = statement;
        copy.type = type;
        copy.blocked = false;
        copy.error = false;
        copy.result = null;
        copy.warning = false;
        copy.submitted = false;
        copy.exceptionMessage = "";
        copy.warnings = null;
        return copy;
    }

    public String getType() {
        return type.toString();
    }
}
