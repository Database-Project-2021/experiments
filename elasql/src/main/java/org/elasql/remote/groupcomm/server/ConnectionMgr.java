/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.remote.groupcomm.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.remote.groupcomm.TimeSync;
import org.elasql.server.Elasql;
import org.elasql.server.Elasql.ServiceType;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());

	public static final int SEQUENCER_ID = VanillaCommServer.getServerCount() - 1;

	private VanillaCommServer commServer;
	private boolean sequencerMode;
	private BlockingQueue<List<Serializable>> tomSendQueue = new LinkedBlockingQueue<List<Serializable>>();
	private boolean areAllServersReady = false;

	// MODIFIED:
	private static final int movingLatencyRange = 500;
	private Map<Integer, Queue<Long>> movingLatency = new HashMap<Integer, Queue<Long>>();
	public Map<Integer, Long> serverLatency = new HashMap<Integer, Long>();
	private Map<Integer, Long> sentSync = new HashMap<Integer, Long>();
	public boolean startSync = false;

	public ConnectionMgr(int id) {
		sequencerMode = Elasql.serverId() == SEQUENCER_ID;
		commServer = new VanillaCommServer(id, this);
		new Thread(null, commServer, "VanillaComm-Server").start();

		// Only the sequencer needs to wait for all servers ready
		if (sequencerMode) {
			waitForServersReady();
			createTomSender();
		}
	}

	// MODIFIED:
	public void sendServerTimeSync(int serverId, long time, boolean isRequest) {
		if (!startSync)
			startSync = true;
		commServer.sendP2pMessage(ProcessType.SERVER, serverId, new TimeSync(time, Elasql.serverId(), isRequest));
		if (isRequest)
			sentSync.put(serverId, time);
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId, new ClientResponse(clientId, rteId, txNum, rs));
	}

	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		commServer.sendTotalOrderMessage(new StoredProcedureCall(-1, -1, pid, pars));
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		commServer.sendP2pMessage(ProcessType.SERVER, nodeId, reading);
	}

	@Override
	public void onServerReady() {
		synchronized (this) {
			areAllServersReady = true;
			this.notifyAll();
		}
	}

	@Override
	public void onServerFailed(int failedServerId) {
		// Do nothing
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.CLIENT) {
			// Normally, the client will only sends its request to the sequencer.
			// However, any other server can also send a total order request.
			// So, we do not need to check if this machine is the sequencer.

			// Transfer the given batch to a list of messages
			StoredProcedureCall[] spcs = (StoredProcedureCall[]) message;
			List<Serializable> tomRequest = new ArrayList<Serializable>(spcs.length);
			for (StoredProcedureCall spc : spcs)
				tomRequest.add(spc);
			try {
				tomSendQueue.put(tomRequest);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (message.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) message;

			if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
				Elasql.migraSysControl()
						.onReceiveMigrationRangeFinishMsg((MigrationRangeFinishMessage) ts.getMetadata());
				return;
			}

			// MODIFIED: Switch between with and without time interval
			// for (Tuple t : ts.getTupleSet()){
			// if (t.timestamp != null)
			// Elasql.remoteRecReceiver().cacheRemoteRecordWithTimeStamp(t);
			// else
			// Elasql.remoteRecReceiver().cacheRemoteRecord(t);
			// }
			for (Tuple t : ts.getTupleSet())
				Elasql.remoteRecReceiver().cacheRemoteRecord(t);

		} else if (message.getClass().equals(TimeSync.class)) {
			// MODIFIED:
			TimeSync ts = (TimeSync) message;
			// Send other server's request back with current timestamp
			if (ts.isRequest()) {
				sendServerTimeSync(ts.getServerID(), System.nanoTime() / 1000, false);
				return;
			} else {
				// Calculate latency, then send another request
				long recvSync = ts.getTime();
				long time_interval = (System.nanoTime() / 1000 - sentSync.get(ts.getServerID())) / 2;
				long latency = sentSync.get(ts.getServerID()) + time_interval - recvSync;

				System.out.printf("A Server Latancy Value: %d, from Server%d to Server%d\n", latency, Elasql.serverId(),
						ts.getServerID());
				if (!movingLatency.containsKey(ts.getServerID())) {
					movingLatency.put(ts.getServerID(), new LinkedList<Long>());
					movingLatency.get(ts.getServerID()).add(latency);
					serverLatency.put(ts.getServerID(), latency);
				} else {
					movingLatency.get(ts.getServerID()).add(latency);
					if (movingLatency.get(ts.getServerID()).size() > movingLatencyRange)
						movingLatency.get(ts.getServerID()).poll();
					long totalLatency = 0L;
					for (long lat : movingLatency.get(ts.getServerID()))
						totalLatency += lat;
					long avgLatency = totalLatency / movingLatency.get(ts.getServerID()).size();
					serverLatency.put(ts.getServerID(), avgLatency);
				}

				sentSync.put(ts.getServerID(), System.nanoTime() / 1000);
				sendServerTimeSync(ts.getServerID(), sentSync.get(ts.getServerID()), true);
			}
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		// The sequencer running with Calvin must receive stored procedure call for
		// planning migrations
		if (sequencerMode && Elasql.SERVICE_TYPE != ServiceType.CALVIN)
			return;

		StoredProcedureCall spc = (StoredProcedureCall) message;
		spc.setTxNum(serialNumber);
		Elasql.scheduler().schedule(spc);
	}

	private void createTomSender() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						List<Serializable> messages = tomSendQueue.take();
						commServer.sendTotalOrderMessages(messages);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();
		;
	}

	private void waitForServersReady() {
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		synchronized (this) {
			try {
				while (!areAllServersReady)
					this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
