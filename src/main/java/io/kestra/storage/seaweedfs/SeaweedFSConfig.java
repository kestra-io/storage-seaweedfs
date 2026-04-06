package io.kestra.storage.seaweedfs;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;

public interface SeaweedFSConfig {

    @PluginProperty(group = "connection")
    @Schema(
        title = "SeaweedFS Filer server host",
        description = "The hostname or IP address of the SeaweedFS filer server (e.g., localhost)"
    )
    String getFilerHost();

    @PluginProperty(group = "connection")
    @Schema(
        title = "SeaweedFS Filer gRPC port",
        description = "The gRPC port of the SeaweedFS filer server (default: 18888)"
    )
    int getFilerPort();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Storage prefix path",
        description = "The root prefix path for all storage operations (e.g., kestra/)"
    )
    String getPrefix();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "Data center replication setting",
        description = "Replication setting for data centers (e.g., 000 for no replication, 001 for 1 copy)"
    )
    String getReplication();
}
