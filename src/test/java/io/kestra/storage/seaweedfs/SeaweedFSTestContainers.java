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

    // Fixed ports required for SeaweedFS:
    // When clients upload files, the filer redirects them to the volume server's
    // advertised address (set via -ip flag). With dynamic ports, SeaweedFS can't
    // know the mapped port, so clients would get wrong addresses.
    // See: https://github.com/seaweedfs/seaweedfs/issues/438
    public static final int MASTER_PORT = 9333;
    public static final int VOLUME_PORT = 8080;
    public static final int FILER_HTTP_PORT = 8888;
    public static final int FILER_GRPC_PORT = 18888;

    public static synchronized void start() {
        if (mainContainer != null && mainContainer.isRunning()) {
            return;
        }

        try {
            System.out.println("Starting SeaweedFS TestContainer (Fixed Port Mapping)...");

            mainContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withCommand(
                    "server",
                    "-ip=localhost",          // Advertise localhost to clients
                    "-ip.bind=0.0.0.0",       // Listen on all interfaces inside container
                    "-dir=/data",
                    "-volume.max=100",
                    "-master.port=" + MASTER_PORT,
                    "-volume.port=" + VOLUME_PORT,
                    "-filer",
                    "-filer.port=" + FILER_HTTP_PORT,
                    "-filer.port.grpc=" + FILER_GRPC_PORT
                )
                .withExposedPorts(MASTER_PORT, VOLUME_PORT, FILER_HTTP_PORT, FILER_GRPC_PORT)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(MASTER_PORT), new ExposedPort(MASTER_PORT)),
                        new PortBinding(Ports.Binding.bindPort(VOLUME_PORT), new ExposedPort(VOLUME_PORT)),
                        new PortBinding(Ports.Binding.bindPort(FILER_HTTP_PORT), new ExposedPort(FILER_HTTP_PORT)),
                        new PortBinding(Ports.Binding.bindPort(FILER_GRPC_PORT), new ExposedPort(FILER_GRPC_PORT))
                    )
                ))
                .waitingFor(Wait.forLogMessage(".*Start Seaweed Filer.*", 1)
                    .withStartupTimeout(Duration.ofSeconds(120)));

            mainContainer.start();

            // Wait for all services to be fully ready
            System.out.println("Waiting for SeaweedFS services to initialize...");
            Thread.sleep(5000);

            System.out.println("SeaweedFS started successfully!");
            System.out.println("  Filer HTTP: localhost:" + FILER_HTTP_PORT);
            System.out.println("  Filer gRPC: localhost:" + FILER_GRPC_PORT);
            System.out.println("  Volume server: localhost:" + VOLUME_PORT);

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