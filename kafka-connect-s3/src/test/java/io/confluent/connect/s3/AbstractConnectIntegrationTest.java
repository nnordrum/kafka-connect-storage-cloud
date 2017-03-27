package io.confluent.connect.s3;

import io.confluent.connect.s3.util.EmbeddedKafkaCluster;
import io.confluent.connect.s3.util.EmbeddedZookeeper;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.net.URI;
import java.util.*;
import java.util.concurrent.Future;

public abstract class AbstractConnectIntegrationTest<K, V> {

    EmbeddedZookeeper zk;
    EmbeddedKafkaCluster kafka;
    Herder herder;
    Connect connect;
    KafkaProducer<K, V> kafkaProducer;

    public AbstractConnectIntegrationTest() {
    }

    public String getDefaultTopic() {
        return getClass().getSimpleName();
    }

    @Before
    public void setUp() throws Exception {
        zk = new EmbeddedZookeeper(2181);
        zk.startup();
        final Properties properties = getKafkaClusterProperties();
        kafka = new EmbeddedKafkaCluster(zk, properties, 9092);
        kafka.startup();
        final StandaloneConfig config = new StandaloneConfig(getStandaloneConfigProperties());
        final RestServer rest = new RestServer(config);
        final URI advertisedUrl = rest.advertisedUrl();
        final String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();
        final Worker worker = new Worker(workerId, Time.SYSTEM, new ConnectorFactory(), config, new FileOffsetBackingStore());
        herder = new StandaloneHerder(worker);
        connect = new Connect(herder, rest);
        connect.start();
        kafkaProducer = new KafkaProducer<>(getProducerProperties());
    }

    @After
    public void tearDown() throws Exception {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.shutdown();
        }
        if (zk != null) {
            zk.shutdown();
        }
    }

    protected Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafka.getBrokerList());
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        return producerProperties;
    }

    protected HashMap<String, String> getStandaloneConfigProperties() {
        final HashMap<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", kafka.getBrokerList());
        props.put("offset.storage.file.filename", "/tmp/connect." + System.currentTimeMillis() + ".offsets");
        props.put("offset.flush.interval.ms", "10000");
        return props;
    }

    protected Properties getKafkaClusterProperties() {
        return new Properties();
    }

    protected Herder.Created<ConnectorInfo> addConnector(final Map<String, String> connectorProps) throws Exception {

        final FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
            @Override
            public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                if (error != null) {
                    log.error("Failed to create job for {}", connectorProps);
                } else {
                    log.info("Created connector {}", info.result().name());
                }
            }
        });
        herder.putConnectorConfig(connectorProps.get(ConnectorConfig.NAME_CONFIG), connectorProps, false, cb);
        return cb.get();
    }

    static class AutocloseableZkClient extends ZkClient implements AutoCloseable {
        public AutocloseableZkClient(EmbeddedZookeeper zk) {
            super(zk.getConnection(), 15 * 1000, 10 * 1000, ZKStringSerializer$.MODULE$);
        }

        @Override
        public void close() throws ZkInterruptedException {
            super.close();
        }
    }

    static class AutocloseableZkUtils extends ZkUtils implements AutoCloseable {
        private final ZkClient zkClient;

        public AutocloseableZkUtils(ZkClient zkClient, ZkConnection zkConnection, boolean isSecure) {
            super(zkClient, zkConnection, isSecure);
            this.zkClient = zkClient;
        }

        @Override
        public void close() {
            super.close();
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    protected AutocloseableZkClient getZkClient() {
        return new AutocloseableZkClient(zk);
    }

    protected AutocloseableZkUtils getZkUtils() {
        return getZkUtils(getZkClient());
    }

    protected AutocloseableZkUtils getZkUtils(ZkClient zkClient) {
        return new AutocloseableZkUtils(zkClient, new ZkConnection(zk.getConnection()), false);
    }

    protected void createTopic(String topicName) {
        createTopic(topicName, 1, 1);
    }

    protected void createTopic(String topicName, int noOfPartitions, int noOfReplication) {
        System.out.println("AbstractConnectIntegrationTest.createTopic#topicName = " + topicName);
        try (final AutocloseableZkUtils zkUtils = getZkUtils()) {
            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, new Properties(), null);
        }
    }

    protected Future<RecordMetadata> sendRecord(K key, V value) {
        return sendRecord(getDefaultTopic(), key, value);
    }
    protected Future<RecordMetadata> sendRecord(String topicName, K key, V value) {
        return kafkaProducer.send(new ProducerRecord<>(topicName, key, value));
    }


    private final Logger log = LoggerFactory.getLogger(getClass());

}
