package io.kestra.storage.seaweedfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface SeaweedFSConfig {

    @PluginProperty
    @Schema(
        title = "Storage mode",
        description = "The storage access mode: FILER for native gRPC access (best performance), " +
            "or S3 for S3-compatible API access via SeaweedFS S3 gateway. Default: FILER"
    )
    StorageMode getMode();

    @PluginProperty
    @Schema(
        title = "SeaweedFS Filer server host",
        description = "The hostname or IP address of the SeaweedFS filer server. " +
            "Required for FILER mode. Example: localhost"
    )
    String getFilerHost();

    @PluginProperty
    @Schema(
        title = "SeaweedFS Filer gRPC port",
        description = "The gRPC port of the SeaweedFS filer server. Default: 18888"
    )
    Integer getFilerPort();

    @PluginProperty
    @Schema(
        title = "Data replication setting",
        description = "Replication setting for FILER mode. " +
            "Format: 'xyz' where x=same rack, y=same data center, z=different data center. " +
            "Example: 000 for no replication, 001 for 1 replica in different DC. Default: 000"
    )
    String getReplication();

    @PluginProperty
    @Schema(
        title = "S3 endpoint URL",
        description = "The SeaweedFS S3 gateway endpoint URL. Required for S3 mode. " +
            "Example: http://localhost:8333"
    )
    String getEndpoint();

    @PluginProperty
    @Schema(
        title = "S3 bucket name",
        description = "The S3 bucket name for storing data. Required for S3 mode. " +
            "The bucket will be auto-created if it doesn't exist."
    )
    String getBucket();

    @PluginProperty
    @Schema(
        title = "S3 access key",
        description = "The S3 access key for authentication. Optional if SeaweedFS S3 gateway " +
            "is running without authentication (allow all mode)."
    )
    String getAccessKey();

    @PluginProperty
    @Schema(
        title = "S3 secret key",
        description = "The S3 secret key for authentication. Optional if SeaweedFS S3 gateway " +
            "is running without authentication (allow all mode)."
    )
    String getSecretKey();

    @PluginProperty
    @Schema(
        title = "S3 region",
        description = "The S3 region. Default: us-east-1"
    )
    String getRegion();

    @PluginProperty
    @Schema(
        title = "Storage prefix path",
        description = "The root prefix for all storage operations in FILER mode. " +
            "Not used in S3 mode (use bucket name for namespacing instead). " +
            "Example: kestra/"
    )
    String getPrefix();
}
