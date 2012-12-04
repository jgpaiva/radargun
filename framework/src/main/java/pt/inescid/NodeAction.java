package pt.inescid;

import java.io.Serializable;

public class NodeAction implements Serializable {
	private static final long serialVersionUID = -5381023483338489914L;

	public final int time;
	public final int nodeIndex;

	NodeAction(int time, int nodeIndex) {
		this.time = time;
		this.nodeIndex = nodeIndex;
	}
}
