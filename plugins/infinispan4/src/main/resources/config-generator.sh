#!/bin/bash

WORKING_DIR=`cd $(dirname $0); cd ..; pwd`

DEST_FILE=${WORKING_DIR}/conf/ispn.xml

STATS="false"
JGR_CONFIG="jgroups.xml"
ISOLATION_LEVEL="REPEATABLE_READ"
CONCURRENCY_LEVEL="32"
WRITE_SKEW="false"
LOCK_TIMEOUT="10000"
LOCKING_MODE="OPTIMISTIC"
TX_PROTOCOL="TWO_PHASE_COMMIT"
TO_CORE_POOL_SIZE="8"
TO_MAX_POOL_SIZE="64"
TO_KEEP_ALIVE="60000"
TO_QUEUE_SIZE="1000"
TO_1PC="false"
DEADLOCK_DETECTION="false"
CLUSTERING_MODE="r"
DIST_NUM_OWNERS="2"
VERSIONS="false"
VERSION_SCHEME="SIMPLE"
CUSTOM_INTERCEPTOR_CHAIN="false"
SYNC_COMMIT="true";
DP_BF_FP="0.01"
DP_MAX_KEYS="1000"
DP_NUMBER_HASHES="10"
DP_TIMEOUT="10000"
L1_ENABLED="false"
L1_REHASH="false"
L1_LIFESPAN="600000"
L1_CLEANUP="600000"
L1_THRESHOLD="-1"

help() {
echo "usage: $0 <options>"
echo "  options:"
echo "    -jgr-config <value>           the filepaht of jgroups configuration file"
echo "                                  default: ${JGR_CONFIG}"
echo ""
echo "    -isolation-level <value>      the transaction isolation level"
echo "                                  default: ${ISOLATION_LEVEL}"
echo ""
echo "    -concurrency-level <value>    the concurrency level for lock containers"
echo "                                  default: ${CONCURRENCY_LEVEL}"
echo ""
echo "    -lock-timeout <value>         maximum time to attempt a particular lock acquisition"
echo "                                  default: ${LOCK_TIMEOUT}"
echo ""
echo "    -to-core-pool-size <value>    total order thread pool configuration"
echo "                                  default: ${TO_CORE_POOL_SIZE}"
echo ""
echo "    -to-max-pool-size <value>     total order thread pool configuration"
echo "                                  default: ${TO_MAX_POOL_SIZE}"
echo ""
echo "    -to-keep-alive <value>        total order thread pool configuration"
echo "                                  default: ${TO_KEEP_ALIVE}"
echo ""
echo "    -to-queue-size <value>        total order thread pool configuration"
echo "                                  default: ${TO_QUEUE_SIZE}"
echo ""
echo "    -clustering-mode <value>      the clustering mode to use" 
echo "                                  values (r -- replicated, d -- distributed, i -- invalidation, l -- local)"
echo "                                  default: ${CLUSTERING_MODE}"
echo ""
echo "    -num-owner <value>            number of owners in distributed mode"
echo "                                  default: ${DIST_NUM_OWNERS}"
echo ""
echo "    -preload-from-db <location>   preload the cache with the data present in the DataBase."
echo "                                  It uses the Berkeley DB and it is located in <location>"
echo ""
echo "    -versioned                    enables the versioned cache"
echo ""
echo "    -write-skew                   enables the write skew check if the isolation level is REPEATABLE_READ"
echo ""
echo "    -to-protocol                  change the commit protocol to Total Order based"
echo ""
echo "    -pb-protocol                  change the commit protocol to Passive Replication based"
echo ""
echo "    -deadlock-detector            enable the deadlock detection mechanism"
echo ""
echo "    -async                        enable asynchronous communication"
echo ""
echo "    -stats                        enable stats collection"
echo ""
echo "    -to-1pc                       enable one phase commit in Total Order protocol (if write skew is enabled)"
echo ""
echo "    -extended-stats               enable the extended statistics reports and collection"
echo ""
echo "    -c50-data-placement           enable the data placement algorithm with C5.0 ML and BF based"
echo ""
echo "    -hm-data-placement            enable the data placement algorithm with Hash Map based"
echo ""
echo "    -bf-data-placement            enable the data placement algorithm with Bloomier Filter based"
echo ""
echo "    -dp-bf-fp                     sets the Bloom Filter false positive probability (from 0 to 1)"
echo "                                  default: ${DP_BF_FP}"
echo ""
echo "    -dp-max-keys                  sets the max number of keys to request in the data placement algorithm"
echo "                                  default: ${DP_MAX_KEYS}"
echo ""
echo "    -dp-nr-hashes                 sets the number of hashes (for Bloomier Filter implementation)"
echo "                                  default: ${DP_NUMBER_HASHES}"
echo ""
echo "    -dp-timeout                   sets the timeout (for Bloomier Filter implementation)"
echo "                                  default: ${DP_TIMEOUT}"
echo ""
echo "    -l1                           enables the L1 cache with the default values. Only in Distributed mode"
echo ""
echo "    -l1-rehash                    entries removed due to a rehash will be moved to L1 rather than"
echo "                                  being removed altogether"
echo "                                  default: disabled"
echo ""
echo "    -l1-lifespan <value>          maximum lifespan of an entry placed in the L1 cache"
echo "                                  default: ${L1_LIFESPAN}"
echo ""
echo "    -l1-cleanup <value>           how often the L1 requestors map is cleaned up of stale items"
echo "                                  default: ${L1_CLEANUP}"
echo ""
echo "    -l1-threshold <value>         determines whether a multicast or a web of unicasts are used when performing"
echo "                                  L1 invalidations. -1 sets to unicast. 0 sets to multicast."
echo "                                  default: ${L1_THRESHOLD}"
echo ""
echo "    -h                            show this message"
}

while [ -n "$1" ]; do
case $1 in
  -h) help; exit 0;;
  -jgr-config) JGR_CONFIG=$2; shift 2;;
  -isolation-level) ISOLATION_LEVEL=$2; shift 2;;
  -concurrency-level) CONCURRENCY_LEVEL=$2; shift 2;;
  -lock-timeout) LOCK_TIMEOUT=$2; shift 2;;
  -to-core-pool-size) TO_CORE_POOL_SIZE=$2; shift 2;;
  -to-max-pool-size) TO_MAX_POOL_SIZE=$2; shift 2;;
  -to-keep-alive) TO_KEEP_ALIVE=$2; shift 2;;
  -clustering-mode) CLUSTERING_MODE=$2; shift 2;;
  -num-owner) DIST_NUM_OWNERS=$2; shift 2;;
  -versioned) VERSIONS="true"; shift 1;;
  -write-skew) WRITE_SKEW="true"; shift 1;;
  -to-protocol) TX_PROTOCOL="TOTAL_ORDER"; shift 1;;
  -pb-protocol) TX_PROTOCOL="PASSIVE_REPLICATION"; shift 1;;
  -deadlock-detector) DEADLOCK_DETECTION="true"; shift 1;;
  -async) ASYNC=1; shift 1;;
  -stats) STATS="true"; shift 1;;
  -to-queue-size) TO_QUEUE_SIZE=$2; shift 2;;
  -to-1pc) TO_1PC="true"; shift 1;;
  -preload-from-db) PRELOAD_LOCATION=$2; shift 2;;
  -extended-stats) CUSTOM_INTERCEPTOR_CHAIN="true"; shift 1;;
  -c50-data-placement) DATA_PLACEMENT="org.infinispan.dataplacement.c50.C50MLObjectLookupFactory"; shift 1;;
  -hm-data-placement) DATA_PLACEMENT="org.infinispan.dataplacement.hm.HashMapObjectLookupFactory"; shift 1;;
  -bf-data-placement) DATA_PLACEMENT="org.infinispan.dataplacement.hm.BloomierFilterObjectLookupFactory"; shift 1;;
  -dp-bf-fp) DP_BF_FP=$2; shift 2;;
  -dp-max-keys) DP_MAX_KEYS=$2; shift 2;;
  -dp-nr-hashes) DP_NUMBER_HASHES=$2; shift 2;;
  -dp-timeout) DP_TIMEOUT=$2; shift 2;;
  -l1) L1_ENABLED="true"; shift 1;;
  -l1-rehash) L1_ENABLED="true"; L1_REHASH="true"; shift 1;;
  -l1-lifespan) L1_ENABLED="true"; L1_LIFESPAN=$2; shift 2;;
  -l1-cleanup) L1_ENABLED="true"; L1_CLEANUP=$2; shift 2;;
  -l1-threshold) L1_ENABLED="true"; L1_THRESHOLD=$2; shift 2;;
  *) echo "WARNING: unknown argument '$1'. It will be ignored" >&2; shift 1;;
  esac
done

if [ "${JGR_CONFIG}" != "jgroups/*" ]; then
JGR_CONFIG="jgroups/${JGR_CONFIG}"
fi

echo "Writing configuration to ${DEST_FILE}"

echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" > ${DEST_FILE}
echo "<infinispan" >> ${DEST_FILE}
echo "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" >> ${DEST_FILE}
echo "        xsi:schemaLocation=\"urn:infinispan:config:5.1 http://www.infinispan.org/schemas/infinispan-config-5.1.xsd\"" >> ${DEST_FILE}
echo "        xmlns=\"urn:infinispan:config:5.1\">" >> ${DEST_FILE}

echo "    <global>" >> ${DEST_FILE}

echo "        <globalJmxStatistics" >> ${DEST_FILE}
echo "                enabled=\"${STATS}\"" >> ${DEST_FILE}
echo "                jmxDomain=\"org.infinispan\"/>" >> ${DEST_FILE}

echo "        <transport" >> ${DEST_FILE}
echo "                clusterName=\"infinispan-cluster\">" >> ${DEST_FILE}
echo "            <properties>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"configurationFile\"" >> ${DEST_FILE}
echo "                        value=\"${JGR_CONFIG}\" />" >> ${DEST_FILE}
echo "            </properties>" >> ${DEST_FILE}
echo "        </transport>" >> ${DEST_FILE}

#Total order protocol (thread pool configuration)
if [ "${TX_PROTOCOL}" == "TOTAL_ORDER" ]; then
echo "        <totalOrderExecutor>" >> ${DEST_FILE}
echo "            <properties>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"minThreads\"" >> ${DEST_FILE}
echo "                        value=\"${TO_CORE_POOL_SIZE}\"/>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"maxThreads\"" >> ${DEST_FILE}
echo "                        value=\"${TO_MAX_POOL_SIZE}\"/>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"queueSize\"" >> ${DEST_FILE}
echo "                        value=\"${TO_QUEUE_SIZE}\"/>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"keepAliveTime\"" >> ${DEST_FILE}
echo "                        value=\"${TO_KEEP_ALIVE}\"/>" >> ${DEST_FILE}
echo "            </properties>" >> ${DEST_FILE}
echo "        </totalOrderExecutor>" >> ${DEST_FILE}
fi

echo "    </global>" >> ${DEST_FILE}

echo "    <default>" >> ${DEST_FILE}

echo "        <locking" >> ${DEST_FILE}
echo "                isolationLevel=\"${ISOLATION_LEVEL}\"" >> ${DEST_FILE}
echo "                concurrencyLevel=\"${CONCURRENCY_LEVEL}\"" >> ${DEST_FILE}
echo "                writeSkewCheck=\"${WRITE_SKEW}\"" >> ${DEST_FILE}
echo "                useLockStriping=\"false\"" >> ${DEST_FILE}
echo "                lockAcquisitionTimeout=\"${LOCK_TIMEOUT}\"/>" >> ${DEST_FILE}

echo "        <transaction" >> ${DEST_FILE}
echo "                transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"" >> ${DEST_FILE}
echo "                useEagerLocking=\"false\"" >> ${DEST_FILE}
echo "                transactionMode=\"TRANSACTIONAL\"" >> ${DEST_FILE}
echo "                syncRollbackPhase=\"false\"" >> ${DEST_FILE}
echo "                cacheStopTimeout=\"30000\"" >> ${DEST_FILE}
echo "                useSynchronization=\"${TO_1PC}\"" >> ${DEST_FILE}
echo "                syncCommitPhase=\"${SYNC_COMMIT}\"" >> ${DEST_FILE}
echo "                lockingMode=\"${LOCKING_MODE}\"" >> ${DEST_FILE}
echo "                eagerLockSingleNode=\"false\"" >> ${DEST_FILE}
echo "                use1PcForAutoCommitTransactions=\"false\"" >> ${DEST_FILE}
echo "                autoCommit=\"true\"" >> ${DEST_FILE}

#Total order protocol
if [ "${TX_PROTOCOL}" == "TOTAL_ORDER" ]; then
echo "                transactionProtocol=\"${TX_PROTOCOL}\"" >> ${DEST_FILE}
fi

#Passive Replication protocol
if [ "${TX_PROTOCOL}" == "PASSIVE_REPLICATION" ]; then
echo "                transactionProtocol=\"${TX_PROTOCOL}\"" >> ${DEST_FILE}
fi

echo "                />" >> ${DEST_FILE}

echo "        <jmxStatistics" >> ${DEST_FILE}
echo "                enabled=\"${STATS}\"/>" >> ${DEST_FILE}

echo "        <deadlockDetection" >> ${DEST_FILE}
echo "                enabled=\"${DEADLOCK_DETECTION}\"/>" >> ${DEST_FILE}

echo "        <clustering mode=\"${CLUSTERING_MODE}\">" >> ${DEST_FILE}

if [ -n "${ASYNC}" ]; then
echo "            <async" >> ${DEST_FILE}
echo "                    replQueueMaxElements=\"1000\"" >> ${DEST_FILE}
echo "                    replQueueClass=\"org.infinispan.remoting.ReplicationQueueImpl\"" >> ${DEST_FILE}
echo "                    useReplQueue=\"false\"" >> ${DEST_FILE}
echo "                    replQueueInterval=\"5000\"" >> ${DEST_FILE}
echo "                    asyncMarshalling=\"false\" />" >> ${DEST_FILE}
else
echo "            <sync" >> ${DEST_FILE}
echo "                    replTimeout=\"15000\" />" >> ${DEST_FILE}
fi

#replicated mode or invalidation
if [ "${CLUSTERING_MODE}" == "r" -o "${CLUSTERING_MODE}" == "i" ]; then
echo "            <stateTransfer" >> ${DEST_FILE}
echo "                    fetchInMemoryState=\"true\"" >> ${DEST_FILE}
echo "                    chunkSize=\"100\"" >> ${DEST_FILE}
echo "                    timeout=\"240000\"/>" >> ${DEST_FILE}
fi

#distributed mode
if [ "${CLUSTERING_MODE}" == "d" ]; then
echo "            <hash" >> ${DEST_FILE}
echo "                    numVirtualNodes=\"100\"" >> ${DEST_FILE}
echo "                    numOwners=\"${DIST_NUM_OWNERS}\"" >> ${DEST_FILE}
echo "                    />" >> ${DEST_FILE}

if [ "${L1_ENABLED}" == "true" ]; then
echo "            <l1" >> ${DEST_FILE}
echo "                    enabled=\"true\"" >> ${DEST_FILE}
echo "                    onRehash=\"${L1_REHASH}\"" >> ${DEST_FILE}
echo "                    lifespan=\"${L1_LIFESPAN}\"" >> ${DEST_FILE}
echo "                    invalidationThreshold=\"${L1_THRESHOLD}\"" >> ${DEST_FILE}
echo "                    cleanupTaskFrequency=\"${L1_CLEANUP}\" />" >> ${DEST_FILE}
fi

fi

echo "        </clustering>" >> ${DEST_FILE}

#customInterceptors
if [ "${CUSTOM_INTERCEPTOR_CHAIN}" == "true" ]; then
if [ "${CLUSTERING_MODE}" == "r" ]; then
echo "        <customInterceptors>" >> ${DEST_FILE}
echo "            <interceptor" >> ${DEST_FILE}
echo "                    after=\"org.infinispan.interceptors.InvocationContextInterceptor\"" >> ${DEST_FILE}
echo "                    class=\"org.infinispan.distribution.wrappers.ReplCustomStatsInterceptor\"/>" >> ${DEST_FILE}
echo "            <interceptor" >> ${DEST_FILE}
echo "                    before=\"org.infinispan.interceptors.NotificationInterceptor\"" >> ${DEST_FILE}
echo "                    class=\"org.infinispan.stats.topK.StreamLibInterceptor\"/>" >> ${DEST_FILE}
echo "        </customInterceptors>" >> ${DEST_FILE}
else if [ "${CLUSTERING_MODE}" == "d" ]; then
echo "        <customInterceptors>" >> ${DEST_FILE}
echo "            <interceptor" >> ${DEST_FILE}
echo "                    after=\"org.infinispan.interceptors.InvocationContextInterceptor\"" >> ${DEST_FILE}
echo "                    class=\"org.infinispan.distribution.wrappers.DistCustomStatsInterceptor\"/>" >> ${DEST_FILE}
echo "            <interceptor" >> ${DEST_FILE}
echo "                    before=\"org.infinispan.interceptors.NotificationInterceptor\"" >> ${DEST_FILE}
echo "                    class=\"org.infinispan.stats.topK.DistributedStreamLibInterceptor\"/>" >> ${DEST_FILE}
echo "        </customInterceptors>" >> ${DEST_FILE}
fi
fi
fi

#put versions if needed
if [ "${VERSIONS}" == "true" ]; then
echo "        <versioning" >> ${DEST_FILE}
echo "                enabled=\"${VERSIONS}\"" >> ${DEST_FILE}
echo "                versioningScheme=\"${VERSION_SCHEME}\" />" >> ${DEST_FILE}
fi

if [ -n "${DATA_PLACEMENT}" ]; then
echo "        <dataPlacement" >> ${DEST_FILE}
echo "                enabled=\"true\"" >> ${DEST_FILE}
echo "                objectLookupFactory=\"${DATA_PLACEMENT}\"" >> ${DEST_FILE}
echo "                maxNumberOfKeysToRequest=\"${DP_MAX_KEYS}\">" >> ${DEST_FILE}
echo "            <properties>" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"keyFeatureManager\"" >> ${DEST_FILE}
echo "                        value=\"org.radargun.cachewrappers.TpccKeyFeaturesManager\"" >> ${DEST_FILE}
echo "                        />" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"location\"" >> ${DEST_FILE}
echo "                        value=\"/tmp/ml\"" >> ${DEST_FILE}
echo "                        />" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"bfFalsePositiveProb\"" >> ${DEST_FILE}
echo "                        value=\"${DP_BF_FP}\"" >> ${DEST_FILE}
echo "                        />" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"numberOfHashes\"" >> ${DEST_FILE}
echo "                        value=\"${DP_NUMBER_HASHES}\"" >> ${DEST_FILE}
echo "                        />" >> ${DEST_FILE}
echo "                <property" >> ${DEST_FILE}
echo "                        name=\"timeout\"" >> ${DEST_FILE}
echo "                        value=\"${DP_TIMEOUT}\"" >> ${DEST_FILE}
echo "                        />" >> ${DEST_FILE}
echo "            </properties>" >> ${DEST_FILE}
echo "        </dataPlacement>" >> ${DEST_FILE}
fi

#preload the data from the database
if [ -n "${PRELOAD_LOCATION}" ]; then
echo "        <loaders" >> ${DEST_FILE}
echo "                passivation=\"false\"" >> ${DEST_FILE}
echo "                shared=\"false\"" >> ${DEST_FILE}
echo "                preload=\"true\">" >> ${DEST_FILE}
echo "            <loader" >> ${DEST_FILE}
echo "                    class=\"org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore\"" >> ${DEST_FILE}
echo "                    fetchPersistentState=\"false"\" >> ${DEST_FILE}
echo "                    ignoreModifications=\"true\"" >> ${DEST_FILE}
echo "                    purgeOnStartup=\"false\">" >> ${DEST_FILE}
echo "                <properties>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"stringsTableNamePrefix\"" >> ${DEST_FILE}
echo "                            value=\"ISPN_STRING_TABLE\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"idColumnName\"" >> ${DEST_FILE}
echo "                            value=\"ID_COLUMN\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"dataColumnName\"" >> ${DEST_FILE}
echo "                            value=\"DATA_COLUMN\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"timestampColumnName\"" >> ${DEST_FILE}
echo "                            value=\"TIMESTAMP_COLUMN\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"timestampColumnType\"" >> ${DEST_FILE}
echo "                            value=\"BIGINT\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"connectionFactoryClass\"" >> ${DEST_FILE}
echo "                            value=\"org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"connectionUrl\"" >> ${DEST_FILE}
echo "                            value=\"jdbc:postgresql:${PRELOAD_LOCATION}\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"userName\"" >> ${DEST_FILE}
echo "                            value=\"postgres\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"password\"" >> ${DEST_FILE}
echo "                            value=\"postgres\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"driverClass\"" >> ${DEST_FILE}
echo "                            value=\"org.postgresql.Driver\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"idColumnType\"" >> ${DEST_FILE}
echo "                            value=\"VARCHAR(255)\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"dataColumnType\"" >> ${DEST_FILE}
echo "                            value=\"BYTEA\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"dropTableOnExit\"" >> ${DEST_FILE}
echo "                            value=\"false\"/>" >> ${DEST_FILE}
echo "                    <property" >> ${DEST_FILE}
echo "                            name=\"createTableOnStart\"" >> ${DEST_FILE}
echo "                            value=\"false\"/>" >> ${DEST_FILE}
echo "                </properties>" >> ${DEST_FILE}
echo "            </loader>" >> ${DEST_FILE}
echo "        </loaders>" >> ${DEST_FILE}
fi

echo "    </default>" >> ${DEST_FILE}
echo "    <namedCache" >> ${DEST_FILE}
echo "            name=\"x\" />" >> ${DEST_FILE}
echo "</infinispan>" >> ${DEST_FILE}

echo "Finished!"