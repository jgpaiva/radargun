package org.radargun.stages;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.radargun.GroupSizeRequest;
import org.radargun.GroupSizeRequest.Type;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedOperation;

/**
 * // TODO: Document this
 * 
 * @author Pedro Ruivo
 * @since 4.0
 */
@MBean(objectName = "Block", description = "Blocks master execution until the unblock method is invoked")
public class KillStage extends AbstractMasterStage {
	private int repeat;
	private int sleepTime;
	private boolean randomKill = false;

	@Override
	public boolean execute() throws Exception {
		ArrayList<String> hosts = masterState.getHostnames();

		sleep();

		for (int it = 0; it < repeat; it++) {
			if (randomKill) {
				int index = new Random().nextInt(hosts.size());
				GroupSizeRequest req = new GroupSizeRequest(GroupSizeRequest.DEFAULT_COMPONENT,
						hosts.get(index),
						GroupSizeRequest.DEFAULT_JMX_PORT, Type.STOP);
				req.doRequest();

				sleep();

				req = new GroupSizeRequest(GroupSizeRequest.DEFAULT_COMPONENT, hosts.get(index),
						GroupSizeRequest.DEFAULT_JMX_PORT, Type.START);
				req.doRequest();

				sleep();
			} else {
				ArrayList<Integer> list = new ArrayList<Integer>();
				for (int i = 0; i < hosts.size(); i++) {
					GroupSizeRequest req = new GroupSizeRequest(GroupSizeRequest.DEFAULT_COMPONENT,
							hosts.get(i),
							GroupSizeRequest.DEFAULT_JMX_PORT, Type.GROUPSIZE);
					Integer[] retVal = req.doRequest();
					if (retVal[0] > retVal[1])
						list.add(i);
				}
				int index = new Random().nextInt(list.size());

				GroupSizeRequest req = new GroupSizeRequest(GroupSizeRequest.DEFAULT_COMPONENT,
						hosts.get(list.get(index)),
						GroupSizeRequest.DEFAULT_JMX_PORT, Type.STOP);
				req.doRequest();

				sleep();

				req = new GroupSizeRequest(GroupSizeRequest.DEFAULT_COMPONENT, hosts.get(list.get(index)),
						GroupSizeRequest.DEFAULT_JMX_PORT, Type.START);
				req.doRequest();

				sleep();
			}
		}
		return true;
	}

	private void sleep() {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setRepeat(int repeat) {
		this.repeat = repeat;
	}

	public void setSleepTime(int sleepTime) {
		this.sleepTime = sleepTime;
	}
	
	public void setRandomKill(boolean value) {
		this.randomKill = value;
	}
}
