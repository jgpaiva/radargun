package org.radargun.stages;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

import pt.inescid.MovementInfo;
import pt.inescid.TraceCreator;

/**
 * Stage that creates arbitrary churn from traces. See also {@link TraceCreator}
 * 
 * @author jgpaiva@gsd.inesc-id.pt
 */
public class StartStopNodesStatsStage extends AbstractDistStage {
	private static final long serialVersionUID = 9021762841032970550L;
	private String movedKeysFolder = "jgpaiva_reports/";
	private String movedKeysFile = movedKeysFolder + "movedkeys.csv";

	public DistStageAck executeOnSlave() {
		DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
		CacheWrapper cacheWrapper = slaveState.getCacheWrapper();

		if (cacheWrapper == null) {
			return logAndReturnError("Cache Wrapper not configured!", result);
		}

		long startTime = (Long) slaveState.get("startTime");
		CacheWrapper slave = slaveState.getCacheWrapper();
		writeStats(slave.getKeysMovedPerRound(), startTime);
		return result;
	}

	private void writeStats(List<MovementInfo> list, long startTime) {
		PrintStream ps = null;
		try {
			new File(movedKeysFolder).mkdir();
			try {
				ps = new PrintStream(movedKeysFile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.exit(-1);
			}

			synchronized (list) {
				for (MovementInfo it : list) {
					long movStartTime = it.getStartTime() - startTime;
					int startKeys = it.getStartKeys();
					int startSize = it.getStartSize();

					Long movEndTime = it.getEndTime() != null ? it.getEndTime() - startTime : null;
					Integer endKeys = it.getEndKeys();
					Integer endSize = it.getEndSize();
					Long newViewID = it.getNewViewId();
					
					Integer numKeys = it.getKeysMoved() != null ? it.getKeysMoved().getSum() : null;

					ps.println(movStartTime + "," + startKeys + "," + startSize + "," + movEndTime + ","
							+ endKeys + "," + endSize + "," + newViewID + "," + numKeys);
				}
				ps.println();
			}
		} finally {
			ps.close();
		}
		log.info("Wrote stats to " + new File(movedKeysFile).getAbsolutePath());
	}

	private DistStageAck logAndReturnError(String errorMessage, DefaultDistStageAck ack) {
		log.warn(errorMessage);
		ack.setError(true);
		ack.setErrorMessage(errorMessage);
		return ack;
	}
}
