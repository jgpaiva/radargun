package org.radargun.stages;

import java.net.URLClassLoader;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.utils.Utils;

/**
 * Stage that strts a CacheWrapper on each slave.
 * 
 * @author Mircea.Markus@jboss.com
 */
public class StartClusterSwitchCHStage extends StartClusterStage {
	private String productName;
	private boolean useSmartClassLoading = true;
	private boolean performClusterSizeValidation = true;
	private boolean staggerSlaveStartup = true;
	private long delayAfterFirstSlaveStarts = 5000;
	private long delayBetweenStartingSlaves = 500;

	private Integer minSize = null;
	private Integer maxSize = null;

	private String config;
	private final int TRY_COUNT = 180;

	private static final String PREV_PRODUCT = "StartClusterStage.previousProduct";
	private static final String CLASS_LOADER = "StartClusterStage.classLoader";

	public StartClusterSwitchCHStage() {
		super.setExitBenchmarkOnSlaveFailure(true);
	}

	public DistStageAck executeOnSlave() {
		DefaultDistStageAck ack = newDefaultStageAck();
		if (slaveState.getCacheWrapper() != null) {
			return logAndReturnError("Wrapper already set on this slave, not starting it again.", ack);
		} else if (minSize == null) {
			return logAndReturnError("minSize not configured!", ack);
		} else if (maxSize == null) {
			return logAndReturnError("maxSize not configured!", ack);
		} else if (config == null) {
			return logAndReturnError("did not load configuration", ack);
		}
		if (slaveIndex != 0)
			return ack;
		
		staggerStartup(slaveIndex, getActiveSlaveCount());

		slaveState.put("config", config);
		log.info("Ack master's StartCluster stage. Local address is: " + slaveState.getLocalAddress()
				+ ". This slave's index is: " + getSlaveIndex());
		CacheWrapper wrapper = null;
		try {
			String plugin = Utils.getCacheWrapperFqnClass(productName);
			wrapper = (CacheWrapper) createInstance(plugin);
			wrapper.setUp(config, false, slaveIndex);
			slaveState.setCacheWrapper(wrapper);
		} catch (Exception e) {
			log.error("Issues while instantiating/starting cache wrapper", e);
			ack.setError(true);
			ack.setRemoteException(e);
			if (wrapper != null) {
				try {
					wrapper.tearDown();
				} catch (Exception ignored) {
				}
			}
			return ack;
		}
		log.info("Successfully started cache wrapper on slave " + getSlaveIndex() + ": " + wrapper);
		wrapper.switchHashFunction(minSize, maxSize);
		log.info("Successfully changed hash function at" + getSlaveIndex());

		return ack;
	}

	public void setMinSize(int minSize) {
		this.minSize = minSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	private Object createInstance(String classFqn) throws Exception {
		if (!useSmartClassLoading) {
			return Class.forName(classFqn).newInstance();
		}
		URLClassLoader classLoader;
		String prevProduct = (String) slaveState.get(PREV_PRODUCT);
		if (prevProduct == null || !prevProduct.equals(productName)) {
			classLoader = createLoader();
			slaveState.put(CLASS_LOADER, classLoader);
			slaveState.put(PREV_PRODUCT, productName);
		} else {// same product and there is a class loader
			classLoader = (URLClassLoader) slaveState.get(CLASS_LOADER);
		}
		log.info("Creating newInstance " + classFqn + " with classloader " + classLoader);
		Thread.currentThread().setContextClassLoader(classLoader);
		return classLoader.loadClass(classFqn).newInstance();
	}

	private URLClassLoader createLoader() throws Exception {
		return Utils.buildProductSpecificClassLoader(productName, this.getClass().getClassLoader());
	}

	public void setConfig(String config) {
		this.config = config;
	}

	public void setUseSmartClassLoading(boolean useSmartClassLoading) {
		this.useSmartClassLoading = useSmartClassLoading;
	}

	public void setPerformCLusterSizeValidation(boolean performCLusterSizeValidation) {
		this.performClusterSizeValidation = performCLusterSizeValidation;
	}

	@Override
	public void initOnMaster(MasterState masterState, int slaveIndex) {
		super.initOnMaster(masterState, slaveIndex);
		this.productName = masterState.nameOfTheCurrentBenchmark();
	}

	@Override
	public String toString() {
		return "StartClusterStage {" +
				"productName='" + productName + '\'' +
				", useSmartClassLoading=" + useSmartClassLoading +
				", config=" + config +
				", " + super.toString();
	}

	public void setStaggerSlaveStartup(boolean staggerSlaveStartup) {
		this.staggerSlaveStartup = staggerSlaveStartup;
	}

	public void setDelayAfterFirstSlaveStarts(long delayAfterFirstSlaveStarts) {
		this.delayAfterFirstSlaveStarts = delayAfterFirstSlaveStarts;
	}

	public void setDelayBetweenStartingSlaves(long delayBetweenSlavesStarts) {
		this.delayBetweenStartingSlaves = delayBetweenSlavesStarts;
	}

	private void staggerStartup(int thisNodeIndex, int activeNodes) {
		if (!staggerSlaveStartup) {
			if (log.isTraceEnabled()) {
				log.trace("Not using slave startup staggering");
			}
			return;
		}
		if (thisNodeIndex == 0) {
			log.info("Startup staggering, cluster size is " + activeNodes
					+ " This is the slave with index 0, not sleeping");
			return;
		}
		long toSleep = delayAfterFirstSlaveStarts + thisNodeIndex * delayBetweenStartingSlaves;
		log.info(" Startup staggering, cluster size is " + activeNodes + ". This is the slave with index "
				+ thisNodeIndex + ". Sleeping for " + toSleep + " millis.");
		try {
			Thread.sleep(toSleep);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Should never happen");
		}
	}

	private DistStageAck logAndReturnError(String errorMessage, DefaultDistStageAck ack) {
		log.warn(errorMessage);
		ack.setError(true);
		ack.setErrorMessage(errorMessage);
		return ack;
	}
}
