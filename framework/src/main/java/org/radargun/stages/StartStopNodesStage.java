package org.radargun.stages;

import java.util.Date;
import java.util.List;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

import pt.inescid.NodeAction;
import pt.inescid.StartNodeAction;
import pt.inescid.StopNodeAction;
import pt.inescid.TraceCreator;

/**
 * Stage that creates arbitrary churn from traces. See also {@link TraceCreator}
 * 
 * @author jgpaiva@gsd.inesc-id.pt
 */
public class StartStopNodesStage extends AbstractDistStage {
	private static final long serialVersionUID = 9021762841032970550L;
	private String movedKeysFolder = "jgpaiva_reports/";
	private String movedKeysFile = movedKeysFolder + "movedkeys.csv";

	public DistStageAck executeOnSlave() {
		DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
		CacheWrapper cacheWrapper = slaveState.getCacheWrapper();
		List<NodeAction> actions = (List<NodeAction>) slaveState.get("myActions");

		if (cacheWrapper == null) {
			return logAndReturnError("Cache Wrapper not configured!", result);
		} else if (actions == null)
			return logAndReturnError("Could not get actions from slave state!", result);

		long startTime = new Date().getTime();
		slaveState.put("startTime",startTime);
		
		boolean up = true;
		CacheWrapper slave = slaveState.getCacheWrapper();

		if (actions.size() > 0)
			log.info("Starting churn. total length will be: " + actions.get(actions.size() - 1).time);
		else
			log.info("Starting churn. will finish immediatelly, empty actions");

		while (!actions.isEmpty()) {
			NodeAction currentAction = actions.remove(0);
			if (currentAction instanceof StopNodeAction) {
				if (up) {
					wait(startTime, currentAction);
					stopNode(slave);
					up = false;
				}
			} else if (currentAction instanceof StartNodeAction) {
				if (!up) {
					wait(startTime, currentAction);
					startNode(slave);
					up = true;
				}
			}
		}
		return result;
	}

	private void stopNode(CacheWrapper slave) {
		try {
			slave.tearDown();
		} catch (Exception e) {
			log.warn(e);
		}
		log.info("stopped node " + slaveIndex);
	}

	private void startNode(CacheWrapper slave) {
		String config = (String) slaveState.get("config");
		try {
			slave.setUp(config, false, slaveIndex);
		} catch (Exception e) {
			log.warn(e);
		}
		log.info("started node " + slaveIndex);
	}

	private void wait(long start, NodeAction currentAction) {
		long diff = 0;
		do { // FIXME: espera activa
			long currentTime = new Date().getTime() - start;
			diff = currentAction.time * 1000 - currentTime;
			if (diff > 2000) {
				try {
					Thread.sleep(diff - 2000);
				} catch (InterruptedException e) {
					log.warn(e);
				}
			}
		} while (diff > 0);
	}

	private DistStageAck logAndReturnError(String errorMessage, DefaultDistStageAck ack) {
		log.warn(errorMessage);
		ack.setError(true);
		ack.setErrorMessage(errorMessage);
		return ack;
	}
}
