package org.radargun;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.HostnameVerifier;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * implements a change workload request to radargun
 * 
 * @author Pedro Ruivo
 * @since 1.1
 */
public class GroupSizeRequest {

	private static final String COMPONENT_PREFIX = "org.radargun:stage=";
	public static final String DEFAULT_COMPONENT = "KillComponent";
	public static final String DEFAULT_JMX_PORT = "9998";

	private final ObjectName benchmarkComponent;
	private final MBeanServerConnection mBeanServerConnection;
	private boolean kill;

	public enum Type {
		GROUPSIZE, STOP, START;
	}

	private final Type myType;
	private String hostname;

	public GroupSizeRequest(String component, String hostname, String port, Type myTipe) throws Exception {
		this.hostname = hostname;
		String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

		JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
		mBeanServerConnection = connector.getMBeanServerConnection();
		benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
		this.myType = myTipe;
	}

	public Integer[] doRequest() {
		if (benchmarkComponent == null) {
			throw new NullPointerException("Component does not exists");
		}

		System.out.println(hostname + " " + myType);

		Object retVal = null;
		try {
			switch (myType) {
			case GROUPSIZE:
				retVal = mBeanServerConnection.invoke(benchmarkComponent, "getGroupSize", new Object[0],
						new String[0]);
				break;
			case STOP:
				mBeanServerConnection.invoke(benchmarkComponent, "stopNode", new Object[0], new String[0]);
				break;
			case START:
				mBeanServerConnection.invoke(benchmarkComponent, "startNode", new Object[0], new String[0]);
				break;
			}
		} catch (Exception e) {
			System.out.println("Failed to invoke changeKeysWorkload");
		}

		return (Integer[]) retVal;
	}
}
