package org.radargun.stages;

import java.util.Date;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedOperation;

/**
 * This stage reset the stats in this cache wrapper on all slaves
 * 
 * @author Pedro Ruivo
 * @since 1.1
 */
@MBean(objectName = "KillComponent")
public class KillSlaveStage extends AbstractDistStage {

	private CacheWrapper cacheWrapper;

	@Override
	public DistStageAck executeOnSlave() {
		DefaultDistStageAck defaultDistStageAck = newDefaultStageAck();

		long startTime = new Date().getTime();
		slaveState.put("startTime",startTime);
		
		cacheWrapper = slaveState.getCacheWrapper();

		return defaultDistStageAck;
	}

	@ManagedOperation
	public Integer[] getGroupSize() {
		return cacheWrapper.getMyGroupSize();
	}

	@ManagedOperation
	public void stopNode() {
		this.stopNode(cacheWrapper);
	}
	
	@ManagedOperation
	public void startNode() {
		this.startNode(cacheWrapper);
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
		ClassLoader cl = (ClassLoader) slaveState.get("cl");
		if (cl != null) Thread.currentThread().setContextClassLoader(cl);
		try {
			slave.setUp(config, false, slaveIndex);
		} catch (Exception e) {
			log.warn(e);
		}
		log.info("started node " + slaveIndex);
	}

	@Override
	public String toString() {
		return "ResetStatsStage{" + super.toString();
	}
}
