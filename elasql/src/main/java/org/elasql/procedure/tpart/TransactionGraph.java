package org.elasql.procedure.tpart;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
// import java.util.HashMap;
import java.util.List;
// import java.util.Map;
// import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// import org.elasql.sql.PrimaryKey;
// import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
// import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public class TransactionGraph {
    class Node {
        long txNum;
        List<Long> dependentTxns;
        long latency;
        long finishTime;
        long predFinishTime;

        Node() {
            this.txNum = 0;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = 0;
            this.finishTime = 0;
            this.predFinishTime = 0;
        }

        Node(long txNum) {
            this.txNum = txNum;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = 0;
            this.finishTime = 0;
            this.predFinishTime = 0;
        }
        Node(long txNum, long latency, long finishTime) {
            this.txNum = txNum;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = latency;
            this.finishTime = finishTime;
            this.predFinishTime = 0;
        }
    }

    // privaste List<Node> allTxn = new ArrayList<Node>();
    private List<Node> seqTxns;
    private ConcurrentHashMap<Long, Node> mapTxns;
    private Lock dsLock = new ReentrantLock();
    // The starting time of the workload in Nano second
    private long startTime;
    private boolean isSetStartTime;
    private Lock startTimeLock = new ReentrantLock();

    public TransactionGraph() {
        this.seqTxns = Collections.synchronizedList(new ArrayList<Node>());
        this.mapTxns = new ConcurrentHashMap<Long, Node>();
        this.startTime = 0;
        this.isSetStartTime = false;
    }

    public void setStartTime(long time){
        startTimeLock.lock();
        try{
            if(!isSetStartTime){
                startTime = time;
                isSetStartTime = true;
            }
        }finally{
            startTimeLock.unlock();
        }
    }

    public void addNode(long txNum, long latency, long finishTime, Collection<Long> dependentTxns) {
        Node node = new Node(txNum, latency, finishTime - startTime);
        for (long key : dependentTxns) {
            node.dependentTxns.add(key);
        }
        
        dsLock.lock();
        try{
            seqTxns.add(node);
            mapTxns.put(txNum, node);
        }finally{
            dsLock.unlock();
        }
    }

    public void computeFinishTime() {
        dsLock.lock();
        try{
            for (Node txn : seqTxns) {
                // Max
                long maxLatency = 0;
                for (long dependentTxn : txn.dependentTxns) {
                    long latency = mapTxns.get(dependentTxn).latency;
                    if (latency > maxLatency) {
                        maxLatency = latency;
                    }
                }
                // Sum
                txn.predFinishTime = maxLatency + txn.latency;
            }
        }finally{
            dsLock.unlock();
        }
    }

    public void generateOutputFile() {
        // tx dependency file
        generateDependencyFile();
    }

    private void generateDependencyFile() {
        String fileName = "./dependency.csv";
        dsLock.lock();
        try{
            try (BufferedWriter writer = createOutputFile(fileName)) {
                // write header
                StringBuilder headerSb = new StringBuilder();
                headerSb.append("TargetTransaction");
                headerSb.append(",");
                headerSb.append("DependencyTransactions");
                headerSb.append("\n");
                writer.append(headerSb.toString());
                sortByTxnNum(seqTxns);
                // write record
                for (Node Txn : seqTxns) {
                    StringBuilder TxnSB = new StringBuilder();
                    TxnSB.append(Txn.txNum);
                    TxnSB.append(",");
                    for (int i = 0; i < Txn.dependentTxns.size(); i++) {
                        TxnSB.append(Txn.dependentTxns.get(i));
                        TxnSB.append(",");
                    }
                    TxnSB.deleteCharAt(TxnSB.length() - 1);
                    TxnSB.append("\n");
                    writer.append(TxnSB.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }finally{
            dsLock.unlock();
        }
    }

    private void sortByTxnNum(List<Node> Txns)
    {
        Collections.sort(Txns, new Comparator<Node>() {

			@Override
			public int compare(Node txn1, Node txn2) {
				return Long.compare(txn1.txNum, txn2.txNum);
			}
		});
    }


    private BufferedWriter createOutputFile(String fileName) throws IOException {
        return new BufferedWriter(new FileWriter(fileName));
    }
}
