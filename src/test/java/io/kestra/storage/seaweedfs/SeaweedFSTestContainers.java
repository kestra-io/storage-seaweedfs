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

    public static final int MASTER_PORT = 19333;
    public static final int VOLUME_PORT = 18080;
    public static final int FILER_HTTP_PORT = 18888;
    public static final int FILER_GRPC_PORT = 19888;

    public static synchronized void start() {
        if (mainContainer != null && mainContainer.isRunning()) {
            return;
        }

        try {
            System.out.println("Starting SeaweedFS TestContainer (Fixed Ports)...");

            mainContainer = new GenericContainer<>(SEAWEEDFS_IMAGE)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                        // Map Host Port -> Container Port 1:1
                        new PortBinding(Ports.Binding.bindPort(MASTER_PORT), new ExposedPort(MASTER_PORT)),
                        new PortBinding(Ports.Binding.bindPort(VOLUME_PORT), new ExposedPort(VOLUME_PORT)),
                        new PortBinding(Ports.Binding.bindPort(FILER_HTTP_PORT), new ExposedPort(FILER_HTTP_PORT)),
                        new PortBinding(Ports.Binding.bindPort(FILER_GRPC_PORT), new ExposedPort(FILER_GRPC_PORT))
                    )
                ))
                .withCommand(
                    "server",
                    // advertise "localhost" so the Java Client (on host) can resolve it
                    "-ip=localhost",
                    // bind to 0.0.0.0 so the Docker Gateway can bridge the connection
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

            mainContainer.start();

            System.out.println("SeaweedFS started!");
            System.out.println("Filer HTTP: " + getFilerUrl());

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
        return mainContainer.getHost();
    }

    public static int getFilerGrpcPort() {
        // Since we mapped 1:1, this is just FILER_GRPC_PORT
        return FILER_GRPC_PORT;
    }

    public static int getFilerHttpPort() {
        return FILER_HTTP_PORT;
    }

    public static String getFilerUrl() {
        return String.format("http://%s:%d", getFilerHost(), getFilerHttpPort());
    }
}