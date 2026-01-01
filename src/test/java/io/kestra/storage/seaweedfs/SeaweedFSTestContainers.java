package io.kestra.storage.seaweedfs;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class SeaweedFSTestContainers {

    private static final DockerImageName SEAWEEDFS_IMAGE = DockerImageName.parse("chrislusf/seaweedfs:latest");
    private static final Network NETWORK = Network.newNetwork();

    private static GenericContainer<?> masterContainer;
    private static GenericContainer<?> volumeContainer;
    private static GenericContainer<?> filerContainer;

    public static final int MASTER_PORT = 9333;
    public static final int VOLUME_PORT = 18080;  // Volume port (grPC will be 18080 + 10000 = 28080)
    public static final int FILER_HTTP_PORT = 8888;
    public static final int FILER_GRPC_PORT = 18888;

    public static synchronized void start() {
        if (masterContainer != null && masterContainer.isRunning()) {
            System.out.println("SeaweedFS containers already running");
            return;
        }

        try {
            System.out.println("Starting SeaweedFS TestContainers...");

            System.out.println("Starting master container...");
            masterContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("seaweedfs-master")
                .withCommand("master", "-ip=seaweedfs-master", "-port=" + MASTER_PORT)
                .withExposedPorts(MASTER_PORT)
                .waitingFor(Wait.forHttp("/cluster/status")
                    .forPort(MASTER_PORT)
                    .withStartupTimeout(Duration.ofSeconds(30)));

            masterContainer.start();
            System.out.println("Master container started on port: " + masterContainer.getMappedPort(MASTER_PORT));

            // Start Volume Server with fixed port and publicUrl pointing to localhost
            System.out.println("Starting volume container...");
            volumeContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("seaweedfs-volume")
                .withCommand(
                    "volume",
                    "-mserver=seaweedfs-master:" + MASTER_PORT,
                    "-port=" + VOLUME_PORT,
                    "-ip=seaweedfs-volume",
                    "-publicUrl=localhost:" + VOLUME_PORT,
                    "-max=100"
                )
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.getHostConfig().withPortBindings(
                        new PortBinding(
                            Ports.Binding.bindPort(VOLUME_PORT),
                            new ExposedPort(VOLUME_PORT)
                        )
                    );
                })
                .dependsOn(masterContainer)
                .waitingFor(Wait.forHttp("/status")
                    .forPort(VOLUME_PORT)
                    .withStartupTimeout(Duration.ofSeconds(30)));

            volumeContainer.start();
            System.out.println("Volume container started with publicUrl=localhost:" + VOLUME_PORT);

            System.out.println("Starting filer container...");
            filerContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("seaweedfs-filer")
                .withCommand(
                    "filer",
                    "-master=seaweedfs-master:" + MASTER_PORT,
                    "-port=" + FILER_HTTP_PORT,
                    "-port.grpc=" + FILER_GRPC_PORT,
                    "-ip=seaweedfs-filer"
                )
                .withExposedPorts(FILER_HTTP_PORT, FILER_GRPC_PORT)
                .dependsOn(volumeContainer)
                .waitingFor(Wait.forHttp("/")
                    .forPort(FILER_HTTP_PORT)
                    .withStartupTimeout(Duration.ofSeconds(30)));

            filerContainer.start();
            System.out.println("Filer container started:");
            System.out.println("  HTTP port: " + filerContainer.getMappedPort(FILER_HTTP_PORT));
            System.out.println("  gRPC port: " + filerContainer.getMappedPort(FILER_GRPC_PORT));
            System.out.println("  Host: " + filerContainer.getHost());

            // Wait a bit for the cluster to stabilize
            Thread.sleep(2000);
            System.out.println("SeaweedFS TestContainers started successfully!");

        } catch (Exception e) {
            System.err.println("ERROR: Failed to start SeaweedFS containers: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to start SeaweedFS TestContainers", e);
        }
    }

    public static synchronized void stop() {
        if (filerContainer != null) {
            filerContainer.stop();
        }
        if (volumeContainer != null) {
            volumeContainer.stop();
        }
        if (masterContainer != null) {
            masterContainer.stop();
        }
        NETWORK.close();
    }

    public static String getFilerHost() {
        return filerContainer != null ? filerContainer.getHost() : "localhost";
    }

    public static int getFilerGrpcPort() {
        return filerContainer != null ? filerContainer.getMappedPort(FILER_GRPC_PORT) : FILER_GRPC_PORT;
    }

    public static int getVolumePort() {
        return VOLUME_PORT;
    }
}
