package io.confluent.connect.s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.*;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.*;
import java.util.concurrent.Future;

public class S3SinkConnectIntegrationTest extends AbstractConnectIntegrationTest<String, JsonNode> {

    public static final String MINIO_PORT = "9000/tcp";
    public static final String MINIO_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";
    public static final String MINIO_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    String mimioContainerId;
    PortBinding mimioPortBinding;
    private MinioClient minioClient;

    @Test
    public void testWithSchema() throws Exception {
        List<Future<RecordMetadata>> futures = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            futures.add(sendRecord("TX_" + i, JsonNodeFactory.instance.objectNode().put("key", i)));
        }
        System.out.println("Sleeping..."); // TODO, is there a way to check if the connector is finished?
        Thread.sleep(10000);
        final MinioClient minioClient = getMinioClient();
        final String bucketName = getDefaultTopic().toLowerCase();
        final Iterable<Result<Item>> results = minioClient.listObjects(bucketName);
        System.out.println("results.iterator().hasNext() = " + results.iterator().hasNext());
        if (!results.iterator().hasNext()) {
            tearDown();
            System.exit(2);
        }
        for (Result<Item> itemResult : results) {
            final Item item = itemResult.get();
            System.out.println("itemResult.get().objectName() = " + item.objectName());
            final String presignedGetObject = minioClient.presignedGetObject(bucketName, item.objectName(), 60 * 60 * 24);
            System.out.println("presignedGetObject = " + presignedGetObject);
        }

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createMimioContainer(getDefaultTopic());
        createS3Connector(getDefaultTopic());
    }

    @Override
    @After
    public void tearDown() throws Exception {
//        try (final DockerClient docker = DefaultDockerClient.fromEnv().build()) {
//            docker.killContainer(mimioContainerId);
//            docker.removeContainer(mimioContainerId);
//            docker.close();
//        }
        super.tearDown();
    }

    protected MinioClient getMinioClient() throws Exception {
        return minioClient;
    }

    protected String getMimioEndpointUrl() {
        System.out.println("mimioPortBinding.hostIp() = " + mimioPortBinding.hostIp());
        System.out.println("mimioPortBinding.hostPort() = " + mimioPortBinding.hostPort());
        final String url = "http://" + ("0.0.0.0".equals(mimioPortBinding.hostIp()) ? "localhost" : mimioPortBinding.hostIp()) + ":" + mimioPortBinding.hostPort();
        System.out.println("url = " + url);
        return url;
    }

    protected void createMimioContainer(final String topicName) throws Exception {
        final String bucketName = topicName.toLowerCase();
        try (final DockerClient docker = DefaultDockerClient.fromEnv().build()) {
            final Map<String, List<PortBinding>> portBindings = new HashMap<>();
            portBindings.put(MINIO_PORT, Collections.singletonList(PortBinding.of("0.0.0.0", 9000)));
            final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
            final ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .env("MINIO_ACCESS_KEY=" + MINIO_ACCESS_KEY
                            , "MINIO_SECRET_KEY=" + MINIO_SECRET_KEY)
                    .image("minio/minio")
                    .exposedPorts(MINIO_PORT)
                    .cmd("server", "/export")
                    .build();
            final ContainerCreation creation = docker.createContainer(containerConfig);
            mimioContainerId = creation.id();
            docker.startContainer(mimioContainerId);
            final ContainerInfo info = docker.inspectContainer(mimioContainerId);
            final ImmutableMap<String, List<PortBinding>> ports = info.networkSettings().ports();
            if (ports != null) {
                mimioPortBinding = ports.get(MINIO_PORT).get(0);
            }
            Thread.sleep(2000);
            minioClient = new MinioClient(new URL(getMimioEndpointUrl()), MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
            boolean exit = true;
            for (int i = 0; i < 5; i++) {
                try {
                    minioClient.listBuckets();
                    exit = false;
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Sleeping for MinioClient Retry");
                Thread.sleep(1000);
            }
            if (exit) {
                System.exit(1);
            }
            System.out.println("creating bucket: " + bucketName);
            getMinioClient().makeBucket(bucketName);
            System.out.println("minioClient.listBuckets() = " + minioClient.listBuckets());
        }
    }

    @Override
    protected HashMap<String, String> getStandaloneConfigProperties() {
        final HashMap<String, String> props = super.getStandaloneConfigProperties();
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");
        props.put("internal.key.converter", "org.apache.kafka.connect.storage.StringConverter");
        props.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("internal.key.converter.schemas.enable", "false");
        props.put("internal.value.converter.schemas.enable", "false");
        return props;
    }

    @Override
    protected Properties getProducerProperties() {
        final Properties producerProperties = super.getProducerProperties();
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        return producerProperties;
    }

    protected void createS3Connector(final String topicName) throws Exception {
        final String bucketName = topicName.toLowerCase();
        createTopic(bucketName);
        final HashMap<String, String> props = new HashMap<>();
        props.put("name", "s3-" + topicName + "-connector");
        props.put("connector.class", "io.confluent.connect.s3.S3SinkConnector");
        props.put("tasks.max", "1");
        props.put("topics", topicName);
        props.put("s3.part.size", "5242880");
        props.put("schema.compatibility", "NONE");
        props.put("storage.class", "io.confluent.connect.s3.storage.S3Storage");
        props.put("format.class", "io.confluent.connect.s3.format.json.JsonFormat");
        props.put("schema.generator.class", "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator");
        props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DefaultPartitioner");
        props.put("flush.size", "1");
        props.put("store.url", getMimioEndpointUrl());
        props.put("s3.region", "us-east-1");
        props.put("s3.bucket.name", bucketName);
        props.put("s3.credentials.provider.class", ProfileCredentialsProvider.class.getName());
        props.put("s3.credentials.provider.profile.name", "minio");

        // TODO need to support specifying the keys in the properties (I know it's "bad")
        //                    .env("MINIO_ACCESS_KEY=" + MINIO_ACCESS_KEY)
        //                    .env("MINIO_SECRET_KEY=" + MINIO_SECRET_KEY)

        final Herder.Created<ConnectorInfo> connectorInfoCreated = addConnector(props);
        System.out.println("connectorInfoCreated.created() = " + connectorInfoCreated.created());
        System.out.println("connectorInfoCreated.result().name() = " + connectorInfoCreated.result().name());
    }

}
