package io.confluent.connect.s3.util;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.utils.SystemTime;
import scala.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class EmbeddedKafkaCluster implements AutoCloseable {
    private final List<Integer> ports;
    private final EmbeddedZookeeper zookeeper;
    private final Properties baseProperties;

    private final String brokerList;

    private final List<KafkaServer> brokers;
    private final List<File> logDirs;

    public EmbeddedKafkaCluster(EmbeddedZookeeper zookeeper) {
        this(zookeeper, new Properties());
    }

    public EmbeddedKafkaCluster(EmbeddedZookeeper zookeeper, Properties baseProperties) {
        this(zookeeper, baseProperties,-1);
    }

    public EmbeddedKafkaCluster(EmbeddedZookeeper zookeeper, Properties baseProperties, Integer... ports) {
        this.zookeeper = zookeeper;
        this.ports = resolvePorts(Arrays.asList(ports));
        this.baseProperties = baseProperties;

        this.brokers = new ArrayList<>();
        this.logDirs = new ArrayList<>();

        this.brokerList = constructBrokerList(this.ports);
    }

    private List<Integer> resolvePorts(List<Integer> ports) {
        List<Integer> resolvedPorts = new ArrayList<>();
        for (Integer port : ports) {
            resolvedPorts.add(resolvePort(port));
        }
        return resolvedPorts;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    private String constructBrokerList(List<Integer> ports) {
        StringBuilder sb = new StringBuilder();
        for (Integer port : ports) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("localhost:").append(port);
        }
        return sb.toString();
    }

    public void startup() {
        for (int i = 0; i < ports.size(); i++) {
            Integer port = ports.get(i);
            File logDir = TestUtils.constructTempDir("kafka-local");

            Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("zookeeper.connect", zookeeper.getConnection());
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", Integer.toString(port));
            properties.setProperty("log.dir", logDir.getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

            KafkaServer broker = startBroker(properties);

            brokers.add(broker);
            logDirs.add(logDir);
        }
    }


    private KafkaServer startBroker(Properties props) {
        KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime(), Option.<String>empty(), new scala.collection.mutable.ArraySeq<KafkaMetricsReporter>(0));
        server.startup();
        return server;
    }

    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(baseProperties);
        props.put("metadata.broker.list", brokerList);
        props.put("zookeeper.connect", zookeeper);
        return props;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public EmbeddedZookeeper getZookeeper() {
        return zookeeper;
    }

    public void shutdown() {
        for (KafkaServer broker : brokers) {
            try {
                broker.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (File logDir : logDirs) {
            try {
                TestUtils.deleteFile(logDir);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
        sb.append("brokerList='").append(brokerList).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
