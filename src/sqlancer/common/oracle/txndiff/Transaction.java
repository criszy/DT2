package sqlancer.common.oracle.txndiff;

import java.util.ArrayList;

public class Transaction {
    int txnId;
    ArrayList<StatementCell> statements;


    public Transaction(int txId) {
        this.txnId = txId;
        statements = new ArrayList<>();
    }


    public void setStatements(ArrayList<StatementCell> statements) {
        this.statements = statements;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(" -- Transaction %d, with statements:\n", txnId));
        if (statements != null) {
            for (StatementCell stmt : statements) {
                sb.append(stmt.statement).append(";\n");
            }
        }
        return sb.toString();
    }
}
