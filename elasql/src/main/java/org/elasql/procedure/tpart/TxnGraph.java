package org.elasql.procedure.tpart;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class TxnGraph {
    class Node{
        long txNum;
        List<Long> dependentTxns;
        long latency;
        long finishTime;
        long predFinishTime;
        
        Node(){
            this.txNum = 0;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = 0;
            this.finishTime = 0;
            this.predFinishTime = 0;
        }
        Node(long txNum){
            this.txNum = txNum;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = 0;
            this.finishTime = 0;
            this.predFinishTime = 0;
        }
        Node(long txNum, long latency, long finishTime){
            this.txNum = txNum;
            this.dependentTxns = new ArrayList<Long>();
            this.latency = latency;
            this.finishTime = finishTime;
            this.predFinishTime = 0;
        }
    }

    // private List<Node> allTxn = new ArrayList<Node>();
    private List<Node> seqTxns;
    private Map<Long, Node> mapTxns;
    
    TxnGraph(){
        this.seqTxns = new ArrayList<Node>();
        this.mapTxns = new HashMap<Long, Node>();
    }

    // public void addTxn(long txNum, long latency, long finishTime, Collection<PrimaryKey> ){

    // }

    public void addNode(long txNum, long latency, long finishTime, Collection<Long> dependentTxns){
        Node node = new Node(txNum, latency, finishTime);
        for(long key : dependentTxns){
            node.dependentTxns.add(key);
        }
        seqTxns.add(node);
        mapTxns.put(txNum, node);
    }

    public void computeFinishTime(){
        for(Node txn : seqTxns){
            // Max
            long maxLatency = 0;
            for(long dependentTxn : txn.dependentTxns){
                long latency = mapTxns.get(dependentTxn).latency;
                if(latency > maxLatency){
                    maxLatency = latency;
                }
            }
            // Sum
            txn.predFinishTime = maxLatency + txn.latency;
        }
    }
    
    public void generateOutputFile(){
        String fileName = "dependency_graph.csv";

        try (BufferedWriter writer = createOutputFile(fileName)) {
			// write header
            StringBuilder headerSb = new StringBuilder();
            headerSb.append("max_latency");
            headerSb.append(",");
            headerSb.append("true_latency");
            headerSb.append("\n");
            writer.append(headerSb.toString());
            // write record
            

		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public void generateThroughputFile(){
        String fileName = "dependency_Throughput.csv";

        try (BufferedWriter writer = createOutputFile(fileName)) {
			// write header
            StringBuilder headerSb = new StringBuilder();
            headerSb.append("TargetTransaction");
            headerSb.append(",");
            headerSb.append("DependecyTrasactions");
            headerSb.append(",");
            headerSb.append("Latency");
            headerSb.append("\n");
            writer.append(headerSb.toString());
            // write record
            for(Node Txn : seqTxns)
            {
                StringBuilder TxnSB = new StringBuilder();
                TxnSB.append(Txn.txNum);
                TxnSB.append(",");
                TxnSB.append(Txn.dependentTxns.get(0));
                TxnSB.append(",");
                TxnSB.append(Txn.latency);
            }
            

		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    private BufferedWriter createOutputFile(String fileName) throws IOException {
		return new BufferedWriter(new FileWriter(fileName));
	}
}
