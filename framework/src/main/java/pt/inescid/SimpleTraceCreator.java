package pt.inescid;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;

import org.radargun.stages.StartStopNodesStage;

/**
 * Generates traces using poison distributions. Traces to be used by
 * {@link StartStopNodesStage}.
 * 
 * @author jgpaiva@gsd.inesc-id.pt
 */
public class SimpleTraceCreator {
	public static final int precision = 1; // FIXME: to set precision, modify
											// what's reading this

	public static final int nNodes = 88;
	public static final double timeToAction = 10 * precision;
	public static final int totalTime = 60 * 60 * precision;
	public static final int repeatRounds = 4;

	public static void main(String args[]) throws Exception {
		int currentTime[] = new int[nNodes];
		ArrayList<NodeAction> actions[] = new ArrayList[nNodes];

		Random r = new Random();
		for (int node = 0; node < nNodes; node++) {
			actions[node] = new ArrayList<NodeAction>();
			actions[node].add(new StartNodeAction(0, node));
			currentTime[node] = 0;
		}

		boolean goingDown = true;
		HashSet<Integer> down = new HashSet<Integer>();
		int currentMax = 1;
		int currentRepeatRounds = repeatRounds;

		for (int time = 0; time < totalTime; time++) {
			if (time % timeToAction != 0)
				continue;
			
			if (down.size() == 0) {
				goingDown = true;
				if(currentRepeatRounds == 1) {
					currentMax++;
					currentRepeatRounds = repeatRounds;
				}else {
					currentRepeatRounds--;
				}
			}
			if (down.size() == currentMax)
				goingDown = false;
			

			if (goingDown) {
				Integer toKill = null;
				do {
					toKill = r.nextInt(nNodes);
				} while (down.contains(toKill));
				down.add(toKill);
				actions[toKill].add(new StopNodeAction(time, toKill));
			} else {
				Integer toRecover = down.iterator().next();
				down.remove(toRecover);
				actions[toRecover].add(new StartNodeAction(time, toRecover));
			}
		}

		int[] actionPointers = new int[nNodes];
		Arrays.fill(actionPointers, 0); // better safe than sorry!
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(System.err);

		boolean changed = false;
		for (int time = 0; time < totalTime; time++) {
			TreeSet<Integer> aliveNodes = new TreeSet<Integer>();
			TreeSet<Integer> deadNodes = new TreeSet<Integer>();
			for (int node = 0; node < nNodes; node++) {
				int index = actionPointers[node];
				NodeAction lastAction = actions[node].get(index);
				NodeAction nextAction = index + 1 < actions[node].size() ? actions[node].get(index + 1)
						: null;
				if (nextAction != null && nextAction.time == time) {
					changed = true;
					actionPointers[node]++;
					writeAction(nextAction, objectOutputStream);
				}
				if (lastAction instanceof StartNodeAction) {
					aliveNodes.add(node);
				} else if (lastAction instanceof StopNodeAction) {
					deadNodes.add(node);
				}
			}
			assert (deadNodes.size() + aliveNodes.size() == nNodes) : deadNodes.size() + " "
					+ aliveNodes.size() + " " + nNodes;
			if (changed) {
				System.out.println(time + " " + aliveNodes.size());
			}
		}
	}

	private static void writeAction(NodeAction action, ObjectOutputStream objectOutputStream)
			throws IOException, ClassNotFoundException {
		objectOutputStream.writeObject(action);
	}

	public static int nextPoisson(double lambda, Random r) {
		int multiplier = 1;
		if (lambda > 500) {
			multiplier = 10;
			lambda /= 10;
		}
		double elambda = Math.exp(-1 * lambda);
		double product = 1;
		int count = 0;
		int result = 0;
		while (product >= elambda) {
			product *= r.nextDouble();
			result = count;
			count++; // keep result one behind
		}
		return (result * multiplier) + 1 + r.nextInt(10);
	}
}
