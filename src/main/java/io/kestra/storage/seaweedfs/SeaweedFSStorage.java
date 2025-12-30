package io.kestra.storage.seaweedfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedInputStream;
import seaweedfs.client.SeaweedOutputStream;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;

import jakarta.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.util.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Plugin
@Plugin.Id("seaweedfs")
public class SeaweedFSStorage implements StorageInterface, SeaweedFSConfig {
    private static final Logger log = LoggerFactory.getLogger(SeaweedFSStorage.class);

    // Static block to register gRPC providers early
    // This fixes issues when META-INF/services files are not properly merged in shadow JARs
    static {
        registerGrpcProviders();
    }

    /**
     * Register required gRPC providers that may be missing due to META-INF/services
     * file conflicts in shadow JARs.
     */
    private static void registerGrpcProviders() {
        // Register DNS NameResolver (fixes "Address types of NameResolver 'unix' not supported")
        try {
            NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
            log.debug("Registered DnsNameResolverProvider for gRPC");
        } catch (Exception e) {
            log.debug("DnsNameResolverProvider registration skipped: {}", e.getMessage());
        }

        // Register PickFirst LoadBalancer (fixes "Could not find policy 'pick_first'")
        try {
            LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
            log.debug("Registered PickFirstLoadBalancerProvider for gRPC");
        } catch (Exception e) {
            log.debug("PickFirstLoadBalancerProvider registration skipped: {}", e.getMessage());
        }
    }

    @Schema(
        title = "SeaweedFS Filer server host",
        description = "The hostname or IP address of the SeaweedFS filer server (e.g., localhost)"
    )
    private String filerHost;

    @Schema(
        title = "SeaweedFS Filer gRPC port",
        description = "The gRPC port of the SeaweedFS filer server (default: 18888)"
    )
    @Builder.Default
    private int filerPort = 18888;

    @Schema(
        title = "Storage prefix path",
        description = "The root prefix path for all storage operations (e.g., kestra/)"
    )
    @Builder.Default
    private String prefix = "kestra/";

    @Schema(
        title = "Data center replication setting",
        description = "Replication setting for data centers (e.g., 000 for no replication, 001 for 1 copy)"
    )
    @Builder.Default
    private String replication = "000";

    @com.fasterxml.jackson.annotation.JsonIgnore
    private transient FilerClient filerClient;

    @Override
    public void init() {
        if (filerHost == null || filerHost.isEmpty()) {
            throw new IllegalArgumentException("Filer host must be configured for SeaweedFS storage");
        }

        try {
            // Ensure gRPC providers are registered
            registerGrpcProviders();

            // Initialize FilerClient with explicit host and port
            String target = filerHost + ":" + filerPort;
            log.info("Initializing SeaweedFS storage with filer: {}", target);
            this.filerClient = new FilerClient(filerHost, filerPort);
            log.info("SeaweedFS storage initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize SeaweedFS storage", e);
            throw new IllegalArgumentException("Failed to initialize SeaweedFS storage: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        // FilerClient doesn't have an explicit close method
        if (filerClient != null) {
            filerClient = null;
        }
    }

    /**
     * Build the full path with tenant, namespace, and prefix.
     */
    private String buildPath(@Nullable String tenantId, @Nullable String namespace, URI uri) {
        StringBuilder path = new StringBuilder("/");

        if (prefix != null && !prefix.isEmpty()) {
            String prefixStr = prefix;
            if (prefixStr.startsWith("/")) {
                prefixStr = prefixStr.substring(1);
            }
            if (!prefixStr.endsWith("/")) {
                prefixStr += "/";
            }
            path.append(prefixStr);
        }

        if (tenantId != null) {
            path.append(tenantId).append("/");
        }

        if (namespace != null) {
            path.append(namespace).append("/");
        }

        String uriPath = uri.getPath();
        if (uriPath.startsWith("/")) {
            uriPath = uriPath.substring(1);
        }
        path.append(uriPath);

        return path.toString();
    }

    /**
     * Split path into directory and filename.
     */
    private String[] splitPath(String fullPath) {
        int lastSlash = fullPath.lastIndexOf('/');
        if (lastSlash <= 0) {
            return new String[]{"/", fullPath};
        }
        String directory = fullPath.substring(0, lastSlash);
        String filename = fullPath.substring(lastSlash + 1);
        return new String[]{directory, filename};
    }

    /**
     * Limit filename to 255 characters to avoid filesystem limitations.
     */
    private String limit(String filename) {
        if (filename.length() <= 255) {
            return filename;
        }

        String extension = "";
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex > 0) {
            extension = filename.substring(dotIndex);
        }

        int maxLength = 255 - extension.length() - 9;
        String truncated = filename.substring(0, Math.max(maxLength, 0));
        return UUID.randomUUID().toString().substring(0, 8) + "_" + truncated + extension;
    }

    @Override
    public InputStream get(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);
        try {
            return new SeaweedInputStream(filerClient, path);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException("File not found: " + path);
            }
            throw new IOException("Failed to get file from SeaweedFS: " + path, e);
        }
    }

    @Override
    public InputStream getInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return get(null, namespace, uri);
    }

    @Override
    public StorageObject getWithMetadata(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);

        try {
            // Get file attributes to extract metadata
            FileAttributes attrs = getAttributes(tenantId, namespace, uri);

            // Get file content
            InputStream inputStream = new SeaweedInputStream(filerClient, path);

            return new StorageObject(attrs.getMetadata(), inputStream);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException("File not found: " + path);
            }
            throw new IOException("Failed to get file with metadata from SeaweedFS: " + path, e);
        }
    }

    @Override
    public List<URI> allByPrefix(@Nullable String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) throws IOException {
        String basePath = buildPath(tenantId, namespace, prefix);
        List<URI> results = new ArrayList<>();
        collectAllFiles(basePath, includeDirectories, results);
        return results;
    }

    private void collectAllFiles(String path, boolean includeDirectories, List<URI> results) throws IOException {
        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);

            for (FilerProto.Entry entry : entries) {
                String fullPath = path.endsWith("/") ? path + entry.getName() : path + "/" + entry.getName();
                boolean isDir = entry.getIsDirectory();

                if (isDir) {
                    if (includeDirectories) {
                        results.add(URI.create(fullPath));
                    }
                    collectAllFiles(fullPath, includeDirectories, results);
                } else {
                    results.add(URI.create(fullPath));
                }
            }
        } catch (Exception e) {
            // Directory might not exist or be empty, just return
            log.debug("Failed to list directory: {}", path, e);
        }
    }

    @Override
    public List<FileAttributes> list(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);

        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);
            List<FileAttributes> result = new ArrayList<>();

            for (FilerProto.Entry entry : entries) {
                Map<String, String> metadata = new HashMap<>();
                // Extract extended attributes if available
                if (entry.getExtendedCount() > 0) {
                    entry.getExtendedMap().forEach((key, value) ->
                        metadata.put(key, new String(value.toByteArray())));
                }

                long lastModifiedTime = entry.getAttributes().getMtime() * 1000; // Convert to milliseconds

                result.add(SeaweedFSFileAttributes.builder()
                    .fileName(entry.getName())
                    .size(entry.getAttributes().getFileSize())
                    .lastModifiedTime(lastModifiedTime)
                    .isDirectory(entry.getIsDirectory())
                    .metadata(metadata)
                    .build());
            }

            return result;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException("Directory not found: " + path);
            }
            throw new IOException("Failed to list directory in SeaweedFS: " + path, e);
        }
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return list(null, namespace, uri);
    }

    @Override
    public FileAttributes getAttributes(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);
        String[] parts = splitPath(path);
        String directory = parts[0];
        String filename = parts[1];

        try {
            FilerProto.Entry entry = filerClient.lookupEntry(directory, filename);

            Map<String, String> metadata = new HashMap<>();
            // Extract extended attributes if available
            if (entry.getExtendedCount() > 0) {
                entry.getExtendedMap().forEach((key, value) ->
                    metadata.put(key, new String(value.toByteArray())));
            }

            long lastModifiedTime = entry.getAttributes().getMtime() * 1000; // Convert to milliseconds

            return SeaweedFSFileAttributes.builder()
                .fileName(entry.getName())
                .size(entry.getAttributes().getFileSize())
                .lastModifiedTime(lastModifiedTime)
                .isDirectory(entry.getIsDirectory())
                .metadata(metadata)
                .build();
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException("File not found: " + path);
            }
            throw new IOException("Failed to get attributes from SeaweedFS: " + path, e);
        }
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        return getAttributes(null, namespace, uri);
    }

    @Override
    public URI put(@Nullable String tenantId, @Nullable String namespace, URI uri, InputStream data) throws IOException {
        return putInternal(tenantId, namespace, uri, data, null);
    }

    @Override
    public URI put(@Nullable String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        return putInternal(tenantId, namespace, uri, storageObject.inputStream(), storageObject.metadata());
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, InputStream data) throws IOException {
        return put(null, namespace, uri, data);
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        return put(null, namespace, uri, storageObject);
    }

    private URI putInternal(@Nullable String tenantId, @Nullable String namespace, URI uri,
                            InputStream data, @Nullable Map<String, String> metadata) throws IOException {
        String path = buildPath(tenantId, namespace, uri);
        log.info("PUT: tenantId={}, namespace={}, uri={}, path={}", tenantId, namespace, uri, path);

        // Apply filename length limit
        String[] pathParts = path.split("/");
        if (pathParts.length > 0) {
            String lastPart = pathParts[pathParts.length - 1];
            String limited = limit(lastPart);
            if (!limited.equals(lastPart)) {
                pathParts[pathParts.length - 1] = limited;
                path = String.join("/", pathParts);
                log.info("PUT: Filename limited from {} to {}, new path: {}", lastPart, limited, path);
            }
        }

        // Ensure parent directory exists
        String[] parts = splitPath(path);
        String directory = parts[0];
        log.info("PUT: Creating directory: {}", directory);
        try {
            filerClient.mkdirs(directory, 0755);
        } catch (Exception e) {
            log.debug("Failed to create directory (might already exist): {}", directory, e);
        }

        try {
            log.info("PUT: Opening SeaweedOutputStream for path: {}", path);
            SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, path);

            // Set replication if configured
            if (replication != null && !replication.isEmpty()) {
                outputStream.setReplication(replication);
                log.info("PUT: Set replication to: {}", replication);
            }

            // Copy data to output stream
            byte[] buffer = new byte[8192];
            int bytesRead;
            long totalBytes = 0;
            while ((bytesRead = data.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }
            outputStream.close();
            log.info("PUT: Successfully wrote {} bytes to path: {}", totalBytes, path);

            // TODO: Store metadata using updateEntry if needed
            if (metadata != null && !metadata.isEmpty()) {
                log.warn("Metadata storage not yet implemented for SeaweedFS");
            }

            // Return the original URI that was passed in, not the internal path
            // This ensures consistency with how Kestra expects storage URIs to work
            log.info("PUT: Returning URI: {}", uri);
            return uri;
        } catch (Exception e) {
            log.error("PUT: Failed to write file to path: {}", path, e);
            throw new IOException("Failed to put file in SeaweedFS: " + path, e);
        }
    }

    @Override
    public boolean delete(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);

        try {
            // Check if it's a directory
            boolean isDirectory = false;
            try {
                FileAttributes attrs = getAttributes(tenantId, namespace, uri);
                isDirectory = attrs.getType() == FileAttributes.FileType.Directory;
            } catch (FileNotFoundException e) {
                return false;
            }

            filerClient.rm(path, isDirectory, true);
            return true;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                return false;
            }
            throw new IOException("Failed to delete file from SeaweedFS: " + path, e);
        }
    }

    @Override
    public boolean deleteInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return delete(null, namespace, uri);
    }

    @Override
    public List<URI> deleteByPrefix(@Nullable String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        List<URI> filesToDelete = allByPrefix(tenantId, namespace, storagePrefix, true);

        if (filesToDelete.isEmpty()) {
            return Collections.emptyList();
        }

        List<URI> deletedFiles = new ArrayList<>();
        Collections.reverse(filesToDelete);

        for (URI fileUri : filesToDelete) {
            try {
                boolean deleted = delete(tenantId, namespace, fileUri);
                if (deleted) {
                    deletedFiles.add(fileUri);
                }
            } catch (IOException e) {
                log.warn("Failed to delete file during deleteByPrefix: {}", fileUri, e);
            }
        }

        return deletedFiles;
    }

    @Override
    public URI createDirectory(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = buildPath(tenantId, namespace, uri);

        try {
            filerClient.mkdirs(path, 0755);
            return uri;
        } catch (Exception e) {
            throw new IOException("Failed to create directory in SeaweedFS: " + path, e);
        }
    }

    @Override
    public URI createInstanceDirectory(@Nullable String namespace, URI uri) throws IOException {
        return createDirectory(null, namespace, uri);
    }

    @Override
    public URI move(@Nullable String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        // SeaweedFS FilerClient doesn't have a native move operation
        // We need to implement it as copy + delete
        String fromPath = buildPath(tenantId, namespace, from);
        String toPath = buildPath(tenantId, namespace, to);

        try {
            // Copy file
            try (InputStream in = new SeaweedInputStream(filerClient, fromPath)) {
                put(tenantId, namespace, to, in);
            }

            // Delete original
            delete(tenantId, namespace, from);

            return to;
        } catch (Exception e) {
            throw new IOException("Failed to move file in SeaweedFS from " + fromPath + " to " + toPath, e);
        }
    }
}