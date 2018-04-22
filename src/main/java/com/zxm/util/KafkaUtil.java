package com.zxm.util;

import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.Map;

import kafka.admin.AdminUtils;
//import kafka.api.TopicMetadata;
//import kafka.javaapi.PartitionMetadata;
//import kafka.javaapi.TopicMetadata;
import org.apache.kafka.common.requests.MetadataResponse.*;
import java.util.*;

import kafka.admin.*;
import scala.collection.*;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;


public class KafkaUtil {

	private static final String ZK_CONNECT = "hadoop11:2181,hadoop12:2181,hadoop13:2181";
	private static final int SESSION_TIMEOUT = 30000;
	private static final int CONNECT_TIMEOUT = 30000;
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

	public static void createTopic(String topic, int partition, int replica, Properties properties) {
		ZkUtils zkUtils = null;
		try {
			zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			if (!AdminUtils.topicExists(zkUtils, topic)) {
				AdminUtils.createTopic(zkUtils, topic, partition, replica, properties,
						AdminUtils.createTopic$default$6());
			} else {
				LOGGER.info("Topic exists...");
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			zkUtils.close();
		}
	}

	public static void modifyTopicConfig(String topic, Properties properties) {
		ZkUtils zkUtils = null;
		try {
			zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			Properties curProp = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
			curProp.putAll(properties);
			AdminUtils.changeTopicConfig(zkUtils, topic, curProp);
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {                                          
			zkUtils.close();
		}
	}

	public static void addPartition(String topic, int partitionAdded, String replicasAdded) {
		ZkUtils zkUtils = null;
		try {
			zkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
			if (topicMetadata != null) {
				List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
				if (partitionMetadatas != null && partitionMetadatas.size() > 0) {
					String[] oldReplicas = new String[partitionMetadatas.size()];
					partitionMetadatas.forEach(pm -> {
						StringBuilder sb = new StringBuilder();
						pm.replicas().forEach(r->{sb.append(r.id()+":");
						});
						oldReplicas[pm.partition()]=sb.deleteCharAt(sb.lastIndexOf(":")).toString();
					});
					StringBuilder oldPartitionSb = new StringBuilder();
					for(int i=-0;i<oldReplicas.length;i++) {
						oldPartitionSb.append(oldReplicas[i]+",");
					}
					
					AdminUtils.addPartitions(zkUtils, topic, partitionMetadatas.size()+partitionAdded,oldPartitionSb.toString()+replicasAdded, 
							true,AdminUtils.addPartitions$default$6());
					
				}
			}

		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			zkUtils.close();
		}
	}

	public static void changePartitionAndReplicas(String topic,int partitionCnt,int replicaCnt) {
		ZkUtils zkUtils = null;
		try {
			zkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
			Seq<BrokerMetadata> brokerMetadat= AdminUtils.getBrokerMetadatas(zkUtils, AdminUtils.getBrokerMetadatas$default$2(),
					AdminUtils.getBrokerMetadatas$default$3());
			scala.collection.Map<Object,Seq<Object>> replicaAssign =
			AdminUtils.assignReplicasToBrokers(brokerMetadat,partitionCnt,replicaCnt,AdminUtils.assignReplicasToBrokers$default$4(),
					AdminUtils.assignReplicasToBrokers$default$5());
			AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic,replicaAssign,
					null,true); 
		}catch (Exception e) {
			LOGGER.error("",e);
		}finally {
			zkUtils.close();
		}
		
	}
	
	public static void deleteTopic(String topic) {
		ZkUtils zkUtils = null;
		try {
			zkUtils= ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT,JaasUtils.isZkSecurityEnabled());
			AdminUtils.deleteTopic(zkUtils, topic);
		}catch(Exception e) {
			LOGGER.error("",e);
		}finally {
			zkUtils.close();
		}
	}
}
