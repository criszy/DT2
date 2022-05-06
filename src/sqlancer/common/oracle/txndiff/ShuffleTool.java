package sqlancer.common.oracle.txndiff;

import com.beust.ah.A;
import sqlancer.Randomly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ShuffleTool {

    public static ArrayList<StatementCell> genOneSubmittedTrace(HashMap<Integer, Transaction> txns) {
        ArrayList<StatementCell> res = new ArrayList<>();
        HashMap<Integer, Integer> num = new HashMap<>();
        ArrayList<Integer> txnID = new ArrayList<>();
        int allStmtsSize = 0;
        for (int i = 1; i <= txns.size(); i++) {
            num.put(i, 0);
            allStmtsSize = allStmtsSize + txns.get(i).statements.size();
            txnID.add(i);
        }
        for (int i = 0; i < allStmtsSize; i++) {
            int index = new Random().nextInt(txnID.size());
            int tid = txnID.get(index);
            if (num.get(tid) == txns.get(tid).statements.size()) {
                i--;
                txnID.remove(index);
                continue;
            }
            int sid = num.get(tid);
            res.add(txns.get(tid).statements.get(sid));
            num.replace(tid, ++sid);
        }
        return res;
    }

    public static ArrayList<ArrayList<StatementCell>> genSeveralSubmittedTrace(HashMap<Integer, Transaction> txns, int count) {
        ArrayList<ArrayList<StatementCell>> res = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ArrayList<StatementCell> one = genOneSubmittedTrace(txns);
            if (res.contains(one)) {
                i--;
                continue;
            }
            res.add(one);
        }
        return res;
    }

    public static ArrayList<ArrayList<StatementCell>> genAllSubmittedTrace(Transaction txn1, Transaction txn2) {
        int n1 = txn1.statements.size(), n2 = txn2.statements.size();
        ArrayList<ArrayList<StatementCell>> res = new ArrayList<>();
        shuffle(res, new ArrayList<>(), txn1.statements, n1, 0, txn2.statements, n2, 0);
        return res;
    }

    public static ArrayList<ArrayList<StatementCell>> genAllSubmittedTrace(HashMap<Integer, Transaction> txns) {
        HashMap<Integer, Integer> num = new HashMap<>();
        for (int i = 1; i <= txns.size(); i++) {
            num.put(i, txns.get(i).statements.size());
        }
        ArrayList<ArrayList<StatementCell>> res = new ArrayList<>();
        shuffle(res, new ArrayList<>(), txns.get(1).statements, num.get(1), 0, txns.get(2).statements, num.get(2), 0);
        for (int i = 3; i <= txns.size(); i++) {
            for (int j = 0; j < res.size(); j++) {
                shuffle(res, new ArrayList<>(), res.get(j), res.get(j).size(), 0, txns.get(i).statements, num.get(i), 0);
            }
        }
        return res;
    }

    public static void shuffle(ArrayList<ArrayList<StatementCell>> res, ArrayList<StatementCell> cur,
                               ArrayList<StatementCell> txn1, int txn1Len, int txn1Idx, ArrayList<StatementCell> txn2,
                               int txn2Len, int txn2Idx) {
        if (txn1Idx == txn1Len && txn2Idx == txn2Len) {
            res.add(new ArrayList<>(cur));
            return;
        }
        if (txn1Idx < txn1Len) {
            cur.add(txn1.get(txn1Idx));
            shuffle(res, cur, txn1, txn1Len, txn1Idx + 1, txn2, txn2Len, txn2Idx);
            cur.remove(cur.size() - 1);
        }
        if (txn2Idx < txn2Len) {
            cur.add(txn2.get(txn2Idx));
            shuffle(res, cur, txn1, txn1Len, txn1Idx, txn2, txn2Len, txn2Idx + 1);
            cur.remove(cur.size() - 1);
        }
    }

    private static int A(int n, int m) {
        int res = 1;
        for (int i = m; i > 0; i--) {
            res *= n;
            n--;
        }
        return res;
    }

    private static int C(int n, int m) {
        if (m > n / 2) {
            m = n - m;
        }
        return A(n, m) / A(m, m);
    }
}
