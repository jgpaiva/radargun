package org.radargun.stages;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OptionalDataException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.keygen2.RadargunKey;

import pt.inescid.NodeAction;
import pt.inescid.StartNodeAction;
import pt.inescid.StopNodeAction;
import pt.inescid.TraceCreator;

/**
 * Stage that creates arbitrary churn from traces. See also {@link TraceCreator}
 * .
 * 
 * @author jgpaiva@gsd.inesc-id.pt
 */
public class StartStopNodesPrepareStage extends AbstractDistStage {
	private static final long serialVersionUID = 9021762841032970550L;

	private String traceFilePath = null;

	public DistStageAck executeOnSlave() {
		DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
		if (traceFilePath == null)
			return logAndReturnError("Trace File Path not defined!", result);

		List<NodeAction> actions = null;
		try {
			actions = getActions();
		} catch (Exception e1) {
			return logAndReturnError("Exception while reading actions:" + e1, result);
		}

		for (Iterator<NodeAction> it = actions.iterator(); it.hasNext();) {
			if (it.next().nodeIndex != this.getSlaveIndex()) {
				it.remove();
			}
		}
		
		slaveState.put("myActions", actions);

		return result;
	}

	private List<NodeAction> getActions() throws IOException, ClassNotFoundException {
		ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(traceFilePath));
		try {
			List<NodeAction> list = new LinkedList<NodeAction>();

			while (true) {
				try {
					Object readObject = objectInputStream.readObject();
					if (readObject instanceof NodeAction) {
						list.add((NodeAction) readObject);
					} else {
						throw new RuntimeException("File is not correctly serialized:"
								+ readObject.getClass().getName());
					}
				} catch (OptionalDataException e) {
					break;
				} catch (EOFException e) {
					break;
				}
			}
			return list;
		} finally {
			objectInputStream.close();
		}
	}

	public void setTraceFilePath(String traceFilePath) {
		this.traceFilePath = traceFilePath;
	}

	private DistStageAck logAndReturnError(String errorMessage, DefaultDistStageAck ack) {
		log.warn(errorMessage);
		ack.setError(true);
		ack.setErrorMessage(errorMessage);
		return ack;
	}
}
