package io.kestra.storage.seaweedfs;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class SeaweedFSTestContainers {

    private static final DockerImageName SEAWEEDFS_IMAGE =
        DockerImageName.parse("chrislusf/seaweedfs:latest");

    private static GenericContainer<?> mainContainer;

    //  Use standard SeaweedFS ports for CI compatibility
    public static final int MASTER_PORT = 9333;
    public static final int VOLUME_PORT = 8080;
    public static final int FILER_HTTP_PORT = 8888;
    public static final int FILER_GRPC_PORT = 18888;

    public static synchronized void start() {
        if (mainContainer != null && mainContainer.isRunning()) {
            return;
        }

        try {
            boolean isLinux = System.getProperty("os.name").toLowerCase().contains("linux");
            System.out.println("Starting SeaweedFS TestContainer (" + (isLinux ? "Host Network" : "Bridge Network") + ")...");

            mainContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withCommand(
                    "server",
                    "-ip=localhost",
                    "-ip.bind=0.0.0.0",
                    "-dir=/data",
                    "-volume.max=100",
                    "-master.port=" + MASTER_PORT,
                    "-volume.port=" + VOLUME_PORT,
                    "-filer",
                    "-filer.port=" + FILER_HTTP_PORT,
                    "-filer.port.grpc=" + FILER_GRPC_PORT
                )
                .waitingFor(
                    Wait.forHttp("/?pretty=y")
                        .forPort(FILER_HTTP_PORT)
                        .withStartupTimeout(Duration.ofSeconds(60))
                );

            if (isLinux) {
                // CI/Linux: Use host networking (no port mapping needed)
                mainContainer.withNetworkMode("host");
            } else {
                // Mac/Windows: Use bridge with 1:1 port mapping
                mainContainer
                    .withExposedPorts(MASTER_PORT, VOLUME_PORT, FILER_HTTP_PORT, FILER_GRPC_PORT)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                        new HostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(MASTER_PORT), new ExposedPort(MASTER_PORT)),
                            new PortBinding(Ports.Binding.bindPort(VOLUME_PORT), new ExposedPort(VOLUME_PORT)),
                            new PortBinding(Ports.Binding.bindPort(FILER_HTTP_PORT), new ExposedPort(FILER_HTTP_PORT)),
                            new PortBinding(Ports.Binding.bindPort(FILER_GRPC_PORT), new ExposedPort(FILER_GRPC_PORT))
                        )
                    ));
            }

            mainContainer.start();

            // Wait for volume server to be fully ready
            Thread.sleep(3000);

            System.out.println("SeaweedFS started successfully!");
            System.out.println("Filer gRPC: " + getFilerHost() + ":" + getFilerGrpcPort());

        } catch (Exception e) {
            stop();
            throw new RuntimeException("Failed to start SeaweedFS", e);
        }
    }

    public static synchronized void stop() {
        if (mainContainer != null) {
            mainContainer.stop();
            mainContainer = null;
        }
    }

    public static String getFilerHost() {
        return "localhost";
    }

    public static int getFilerGrpcPort() {
        return FILER_GRPC_PORT;
    }

    public static int getFilerHttpPort() {
        return FILER_HTTP_PORT;
    }
}