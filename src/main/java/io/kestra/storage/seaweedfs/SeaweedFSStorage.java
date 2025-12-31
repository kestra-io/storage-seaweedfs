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
    private static final int MAX_OBJECT_NAME_LENGTH = 255;

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
        try {
            NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
            log.debug("Registered DnsNameResolverProvider for gRPC");
        } catch (Exception e) {
            log.debug("DnsNameResolverProvider registration skipped: {}", e.getMessage());
        }

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
            registerGrpcProviders();
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
        if (filerClient != null) {
            filerClient = null;
        }
    }

    public String getPath(@Nullable String tenantId, URI uri) {
        StringBuilder path = new StringBuilder();

        // Add prefix (without leading slash)
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

        // Add tenant if present
        if (tenantId != null && !tenantId.isEmpty()) {
            path.append(tenantId).append("/");
        }

        String uriPath = uri.getPath();
        if (uriPath.startsWith("/")) {
            uriPath = uriPath.substring(1);
        }
        path.append(uriPath);

        return path.toString();
    }

    /**
     * Build path without tenant (for instance resources).
     */
    public String getPath(URI uri) {
        return getPath(null, uri);
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

    private URI limit(URI uri) throws IOException {
        if (uri == null) {
            return null;
        }

        String path = uri.getPath();
        String objectName = path.contains("/") ? path.substring(path.lastIndexOf("/") + 1) : path;

        if (objectName.length() > MAX_OBJECT_NAME_LENGTH) {
            objectName = objectName.substring(objectName.length() - MAX_OBJECT_NAME_LENGTH + 6);
            String prefix = UUID.randomUUID().toString().substring(0, 5).toLowerCase();
            String newPath = (path.contains("/") ? path.substring(0, path.lastIndexOf("/") + 1) : "") + prefix + "-" + objectName;
            try {
                return new URI(uri.getScheme(), uri.getHost(), newPath, uri.getFragment());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return uri;
    }

    /**
     * Create parent directories for a path.
     */
    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/") + 1);
        }

        if (path.isEmpty() || path.equals("/")) {
            return;
        }

        try {
            filerClient.mkdirs(path, 0755);
        } catch (Exception e) {
            log.debug("Failed to create directory (might already exist): {}", path, e);
        }
    }

    @Override
    public InputStream get(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return getFromSeaweedFS(uri, path);
    }

    @Override
    public InputStream getInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        return getFromSeaweedFS(uri, path);
    }

    private InputStream getFromSeaweedFS(URI uri, String path) throws IOException {
        try {
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            log.debug("Getting file from SeaweedFS: {}", path);
            return new SeaweedInputStream(filerClient, path);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException(uri.toString() + " (File not found)");
            }
            throw new IOException("Failed to get file from SeaweedFS: " + path, e);
        }
    }

    @Override
    public StorageObject getWithMetadata(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);

        try {
            // Get file attributes to extract metadata
            FileAttributes attrs = getAttributes(tenantId, namespace, uri);

            InputStream inputStream = getFromSeaweedFS(uri, path);

            return new StorageObject(attrs.getMetadata(), inputStream);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to get file with metadata from SeaweedFS: " + path, e);
        }
    }

    @Override
    public URI put(@Nullable String tenantId, @Nullable String namespace, URI uri, InputStream data) throws IOException {
        URI limited = limit(uri);
        return putToSeaweedFS(limited, new StorageObject(null, data), getPath(tenantId, limited));
    }

    @Override
    public URI put(@Nullable String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        URI limited = limit(uri);
        return putToSeaweedFS(limited, storageObject, getPath(tenantId, limited));
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, InputStream data) throws IOException {
        URI limited = limit(uri);
        return putToSeaweedFS(limited, new StorageObject(null, data), getPath(limited));
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        URI limited = limit(uri);
        return putToSeaweedFS(limited, storageObject, getPath(limited));
    }

    private URI putToSeaweedFS(URI uri, StorageObject storageObject, String path) throws IOException {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        mkdirs(path);

        log.debug("Putting file to SeaweedFS: {}", path);

        try (InputStream data = storageObject.inputStream()) {
            SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, path);

            if (replication != null && !replication.isEmpty()) {
                outputStream.setReplication(replication);
            }

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = data.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.close();

            // TODO: Store metadata if needed
            if (storageObject.metadata() != null && !storageObject.metadata().isEmpty()) {
                log.debug("Metadata storage not yet implemented for SeaweedFS");
            }

        } catch (Exception e) {
            throw new IOException("Failed to put file in SeaweedFS: " + path, e);
        }
        return URI.create("kestra://" + uri.getPath());
    }

    @Override
    public List<URI> allByPrefix(@Nullable String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) throws IOException {
        String internalStoragePrefix = getPath(tenantId, prefix);
        if (!internalStoragePrefix.startsWith("/")) {
            internalStoragePrefix = "/" + internalStoragePrefix;
        }

        List<URI> results = new ArrayList<>();
        collectAllFiles(internalStoragePrefix, includeDirectories, results, internalStoragePrefix, prefix.getPath());
        return results;
    }

    private void collectAllFiles(String path, boolean includeDirectories, List<URI> results,
                                 String basePrefix, String uriPrefix) throws IOException {
        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);

            for (FilerProto.Entry entry : entries) {
                String fullPath = path.endsWith("/") ? path + entry.getName() : path + "/" + entry.getName();
                boolean isDir = entry.getIsDirectory();

                if (isDir) {
                    if (includeDirectories) {
                        String relativePath = fullPath.substring(basePrefix.length());
                        results.add(URI.create("kestra://" + uriPrefix + relativePath));
                    }
                    collectAllFiles(fullPath, includeDirectories, results, basePrefix, uriPrefix);
                } else {
                    String relativePath = fullPath.substring(basePrefix.length());
                    results.add(URI.create("kestra://" + uriPrefix + relativePath));
                }
            }
        } catch (Exception e) {
            log.debug("Failed to list directory: {}", path, e);
        }
    }

    @Override
    public List<FileAttributes> list(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);
            List<FileAttributes> result = new ArrayList<>();

            for (FilerProto.Entry entry : entries) {
                Map<String, String> metadata = new HashMap<>();
                if (entry.getExtendedCount() > 0) {
                    entry.getExtendedMap().forEach((key, value) ->
                        metadata.put(key, new String(value.toByteArray())));
                }

                long lastModifiedTime = entry.getAttributes().getMtime() * 1000;

                result.add(SeaweedFSFileAttributes.builder()
                    .fileName(entry.getName())
                    .size(entry.getAttributes().getFileSize())
                    .lastModifiedTime(lastModifiedTime)
                    .isDirectory(entry.getIsDirectory())
                    .metadata(metadata)
                    .build());
            }

            if (result.isEmpty()) {
                this.getAttributes(tenantId, namespace, uri);
            }

            return result;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to list directory in SeaweedFS: " + path, e);
        }
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return list(null, namespace, uri);
    }

    @Override
    public boolean exists(@Nullable String tenantId, @Nullable String namespace, URI uri) {
        try {
            getAttributes(tenantId, namespace, uri);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean existsInstanceResource(@Nullable String namespace, URI uri) {
        try {
            getInstanceAttributes(namespace, uri);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return getFileAttributes(path, uri.toString());
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return getFileAttributes(path, uri.toString());
    }

    private FileAttributes getFileAttributes(String path, String uriString) throws IOException {
        String[] parts = splitPath(path);
        String directory = parts[0];
        String filename = parts[1];

        try {
            FilerProto.Entry entry = filerClient.lookupEntry(directory, filename);

            Map<String, String> metadata = new HashMap<>();
            if (entry.getExtendedCount() > 0) {
                entry.getExtendedMap().forEach((key, value) ->
                    metadata.put(key, new String(value.toByteArray())));
            }

            long lastModifiedTime = entry.getAttributes().getMtime() * 1000;

            return SeaweedFSFileAttributes.builder()
                .fileName(entry.getName())
                .size(entry.getAttributes().getFileSize())
                .lastModifiedTime(lastModifiedTime)
                .isDirectory(entry.getIsDirectory())
                .metadata(metadata)
                .build();
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException(uriString + " (File not found)");
            }
            throw new IOException("Failed to get attributes from SeaweedFS: " + path, e);
        }
    }

    @Override
    public boolean delete(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }

        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            URI dirUri = uri.getPath().endsWith("/") ? uri : URI.create(uri + "/");
            return !deleteByPrefix(tenantId, namespace, dirUri).isEmpty();
        }

        try {
            filerClient.rm(path, false, false);
            return true;
        } catch (Exception e) {
            log.warn("Failed to delete file: {}", path, e);
            return false;
        }
    }

    @Override
    public boolean deleteInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return delete(null, namespace, uri);
    }

    @Override
    public List<URI> deleteByPrefix(@Nullable String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        List<URI> allFiles = allByPrefix(tenantId, namespace, storagePrefix, true);

        if (allFiles.isEmpty()) {
            return Collections.emptyList();
        }

        List<URI> deletedFiles = new ArrayList<>();
        // Delete in reverse order (deepest first)
        Collections.reverse(allFiles);

        for (URI fileUri : allFiles) {
            try {
                String path = getPath(tenantId, fileUri);
                if (!path.startsWith("/")) {
                    path = "/" + path;
                }
                filerClient.rm(path, false, false);
                deletedFiles.add(fileUri);
            } catch (Exception e) {
                log.warn("Failed to delete file during deleteByPrefix: {}", fileUri, e);
            }
        }

        return deletedFiles;
    }

    @Override
    public URI createDirectory(@Nullable String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        try {
            filerClient.mkdirs(path, 0755);
        } catch (Exception e) {
            throw new IOException("Failed to create directory in SeaweedFS: " + path, e);
        }
        return URI.create("kestra://" + uri.getPath());
    }

    @Override
    public URI createInstanceDirectory(@Nullable String namespace, URI uri) throws IOException {
        return createDirectory(null, namespace, uri);
    }

    @Override
    public URI move(@Nullable String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        String sourcePath = getPath(tenantId, from);
        String destPath = getPath(tenantId, to);

        if (!sourcePath.startsWith("/")) {
            sourcePath = "/" + sourcePath;
        }
        if (!destPath.startsWith("/")) {
            destPath = "/" + destPath;
        }

        try {
            FileAttributes attributes = getAttributes(tenantId, namespace, from);

            if (attributes.getType() == FileAttributes.FileType.Directory) {
                // Move directory contents
                List<URI> files = allByPrefix(tenantId, namespace, from, true);
                for (URI fileUri : files) {
                    String relativePath = fileUri.getPath().substring(from.getPath().length());
                    URI destFileUri = URI.create(to.getPath() + relativePath);
                    moveSingleFile(getPath(tenantId, fileUri), getPath(tenantId, destFileUri));
                }
                deleteByPrefix(tenantId, namespace, from);
            } else {
                moveSingleFile(sourcePath, destPath);
            }
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to move file in SeaweedFS from " + from + " to " + to, e);
        }

        return URI.create("kestra://" + to.getPath());
    }

    private void moveSingleFile(String source, String dest) throws IOException {
        if (!source.startsWith("/")) {
            source = "/" + source;
        }
        if (!dest.startsWith("/")) {
            dest = "/" + dest;
        }

        mkdirs(dest);

        try (InputStream in = new SeaweedInputStream(filerClient, source)) {
            SeaweedOutputStream out = new SeaweedOutputStream(filerClient, dest);
            if (replication != null && !replication.isEmpty()) {
                out.setReplication(replication);
            }
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.close();
        }

        try {
            filerClient.rm(source, false, false);
        } catch (Exception e) {
            log.warn("Failed to delete source file after move: {}", source, e);
        }
    }
}
