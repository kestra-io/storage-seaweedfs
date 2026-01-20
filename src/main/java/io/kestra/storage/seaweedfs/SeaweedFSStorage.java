package io.kestra.storage.seaweedfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;
import io.kestra.storage.s3.S3Storage;
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
import java.nio.charset.StandardCharsets;
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
    static {
        registerGrpcProviders();
    }

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
        title = "Storage mode",
        description = "The storage access mode: FILER for native gRPC access (best performance), " +
            "or S3 for S3-compatible API access via SeaweedFS S3 gateway. Default: FILER"
    )
    @Builder.Default
    private StorageMode mode = StorageMode.FILER;

    @Schema(
        title = "SeaweedFS Filer server host",
        description = "The hostname or IP address of the SeaweedFS filer server. " +
            "Required for FILER mode. Example: localhost"
    )
    private String filerHost;

    @Schema(
        title = "SeaweedFS Filer gRPC port",
        description = "The gRPC port of the SeaweedFS filer server. Default: 18888"
    )
    @Builder.Default
    private Integer filerPort = 18888;

    @Schema(
        title = "Data replication setting",
        description = "Replication setting for FILER mode. Default: 000"
    )
    @Builder.Default
    private String replication = "000";

    @Schema(
        title = "S3 endpoint URL",
        description = "The SeaweedFS S3 gateway endpoint URL. Required for S3 mode."
    )
    private String endpoint;

    @Schema(
        title = "S3 bucket name",
        description = "The S3 bucket name for storing data. Required for S3 mode."
    )
    private String bucket;

    @Schema(
        title = "S3 access key",
        description = "The S3 access key for authentication. Optional if no auth."
    )
    private String accessKey;

    @Schema(
        title = "S3 secret key",
        description = "The S3 secret key for authentication. Optional if no auth."
    )
    private String secretKey;

    @Schema(
        title = "S3 region",
        description = "The S3 region. Default: us-east-1"
    )
    @Builder.Default
    private String region = "us-east-1";

    // ==================== Filer Mode Configuration (continued) ====================

    @Schema(
        title = "Storage prefix path",
        description = "The root prefix for all storage operations in FILER mode. " +
            "Not used in S3 mode (use bucket name for namespacing instead)."
    )
    @Builder.Default
    private String prefix = "";

    // ==================== Internal Clients ====================

    @com.fasterxml.jackson.annotation.JsonIgnore
    private transient FilerClient filerClient;

    @com.fasterxml.jackson.annotation.JsonIgnore
    private transient S3Storage s3Storage;

    // ==================== Initialization ====================

    @Override
    public void init() {
        if (mode == StorageMode.S3) {
            initS3Mode();
        } else {
            initFilerMode();
        }
    }

    private void initFilerMode() {
        if (filerHost == null || filerHost.isEmpty()) {
            throw new IllegalArgumentException("filerHost must be configured for FILER mode");
        }

        try {
            registerGrpcProviders();
            int port = filerPort != null ? filerPort : 18888;
            String target = filerHost + ":" + port;
            log.info("Initializing SeaweedFS storage in FILER mode with filer: {}", target);
            this.filerClient = new FilerClient(filerHost, port);
            log.info("SeaweedFS FILER mode initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize SeaweedFS FILER mode", e);
            throw new IllegalArgumentException("Failed to initialize SeaweedFS storage: " + e.getMessage(), e);
        }
    }

    private void initS3Mode() {
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException("endpoint must be configured for S3 mode");
        }
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("bucket must be configured for S3 mode");
        }

        try {
            log.info("Initializing SeaweedFS storage in S3 mode with endpoint: {}, bucket: {}", endpoint, bucket);

            var builder = S3Storage.builder()
                .endpoint(endpoint)
                .bucket(bucket)
                .forcePathStyle(true);  // SeaweedFS requires path-style access

            if (region != null && !region.isEmpty()) {
                builder.region(region);
            }

            if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
                builder.accessKey(accessKey).secretKey(secretKey);
            }

            this.s3Storage = builder.build();
            this.s3Storage.init();

            log.info("SeaweedFS S3 mode initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize SeaweedFS S3 mode", e);
            throw new IllegalArgumentException("Failed to initialize SeaweedFS S3 storage: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (filerClient != null) {
            filerClient = null;
        }
        if (s3Storage != null) {
            s3Storage.close();
            s3Storage = null;
        }
    }

    private boolean isS3Mode() {
        return mode == StorageMode.S3 && s3Storage != null;
    }

    // ==================== Path Utilities (Filer Mode) ====================

    private void validateNoPathTraversal(URI uri) {
        if (uri == null) return;
        String path = uri.getPath();
        if (path != null && path.contains("..")) {
            throw new IllegalArgumentException("Path traversal is not allowed: " + path);
        }
    }

    private String[] splitPath(String fullPath) {
        int lastSlash = fullPath.lastIndexOf('/');
        if (lastSlash <= 0) {
            return new String[]{"/", fullPath};
        }
        return new String[]{fullPath.substring(0, lastSlash), fullPath.substring(lastSlash + 1)};
    }

    private URI limit(URI uri) throws IOException {
        if (uri == null) return null;

        String path = uri.getPath();
        String objectName = path.contains("/") ? path.substring(path.lastIndexOf("/") + 1) : path;

        if (objectName.length() > MAX_OBJECT_NAME_LENGTH) {
            objectName = objectName.substring(objectName.length() - MAX_OBJECT_NAME_LENGTH + 6);
            String uuidPrefix = UUID.randomUUID().toString().substring(0, 5).toLowerCase();
            String newPath = (path.contains("/") ? path.substring(0, path.lastIndexOf("/") + 1) : "") + uuidPrefix + "-" + objectName;
            try {
                return new URI(uri.getScheme(), uri.getHost(), newPath, uri.getFragment());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return uri;
    }

    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/") + 1);
        }
        if (path.isEmpty() || path.equals("/")) return;

        try {
            filerClient.mkdirs(path, 0755);
        } catch (Exception e) {
            log.debug("Failed to create directory (might already exist): {}", path, e);
        }
    }

    private void storeMetadata(String path, Map<String, String> metadata) throws IOException {
        if (metadata == null || metadata.isEmpty()) return;

        try {
            String[] parts = splitPath(path);
            FilerProto.Entry entry = filerClient.lookupEntry(parts[0], parts[1]);
            if (entry == null) {
                log.warn("Cannot store metadata for non-existent file: {}", path);
                return;
            }

            FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
            entryBuilder.clearExtended();
            for (Map.Entry<String, String> metaEntry : metadata.entrySet()) {
                entryBuilder.putExtended(
                    metaEntry.getKey(),
                    com.google.protobuf.ByteString.copyFrom(metaEntry.getValue().getBytes(StandardCharsets.UTF_8))
                );
            }
            filerClient.updateEntry(parts[0], entryBuilder.build());
            log.debug("Stored {} metadata entries for: {}", metadata.size(), path);
        } catch (Exception e) {
            log.warn("Failed to store metadata for: {}", path, e);
        }
    }

    private String encodePathForUri(String path) {
        if (path == null || path.isEmpty()) return path;

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (isValidUriPathChar(c)) {
                result.append(c);
            } else {
                byte[] bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                for (byte b : bytes) {
                    result.append('%');
                    result.append(String.format("%02X", b & 0xFF));
                }
            }
        }
        return result.toString();
    }

    private boolean isValidUriPathChar(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '-' || c == '.' || c == '_' || c == '~' || c == ':' || c == '@' ||
            c == '!' || c == '$' || c == '&' || c == '\'' || c == '*' || c == '+' ||
            c == ',' || c == ';' || c == '=' || c == '/';
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.get(tenantId, namespace, uri);
        }
        return getFromFiler(uri, getPath(tenantId, uri));
    }

    @Override
    public InputStream getInstanceResource(String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.getInstanceResource(namespace, uri);
        }
        return getFromFiler(uri, getPath(uri));
    }

    private InputStream getFromFiler(URI uri, String path) throws IOException {
        try {
            if (!path.startsWith("/")) path = "/" + path;
            log.debug("Getting file from SeaweedFS Filer: {}", path);
            return new BufferedSeaweedInputStream(filerClient, path);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(uri.toString() + " (File not found)");
        } catch (Exception e) {
            if (e.getCause() instanceof FileNotFoundException ||
                (e.getMessage() != null && e.getMessage().contains("NOT_FOUND"))) {
                throw new FileNotFoundException(uri.toString() + " (File not found)");
            }
            throw new IOException("Failed to get file from SeaweedFS: " + path, e);
        }
    }

    private static class BufferedSeaweedInputStream extends InputStream {
        private final SeaweedInputStream delegate;
        private static final int BUFFER_SIZE = 8192;

        public BufferedSeaweedInputStream(FilerClient filerClient, String path) throws IOException {
            this.delegate = new SeaweedInputStream(filerClient, path);
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int result = read(b, 0, 1);
            return result == -1 ? -1 : b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b == null) throw new NullPointerException();
            if (off < 0 || len < 0 || len > b.length - off) throw new IndexOutOfBoundsException();
            if (len == 0) return 0;

            try {
                int totalRead = 0;
                int remaining = len;

                while (remaining > 0) {
                    int chunkSize = Math.min(remaining, BUFFER_SIZE);
                    byte[] chunk = new byte[chunkSize];
                    int bytesRead = delegate.read(chunk, 0, chunkSize);
                    if (bytesRead == -1) return totalRead == 0 ? -1 : totalRead;

                    System.arraycopy(chunk, 0, b, off + totalRead, bytesRead);
                    totalRead += bytesRead;
                    remaining -= bytesRead;
                    if (bytesRead < chunkSize) break;
                }
                return totalRead;
            } catch (IllegalArgumentException e) {
                if (e.getMessage() != null && e.getMessage().contains("offset greater than length")) {
                    return -1;
                }
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException { delegate.close(); }

        @Override
        public int available() throws IOException { return delegate.available(); }
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.getWithMetadata(tenantId, namespace, uri);
        }

        String path = getPath(tenantId, uri);
        FileAttributes attrs = getAttributes(tenantId, namespace, uri);
        InputStream inputStream = getFromFiler(uri, path);
        return new StorageObject(attrs.getMetadata(), inputStream);
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, InputStream data) throws IOException {
        if (isS3Mode()) {
            return s3Storage.put(tenantId, namespace, uri, data);
        }
        URI limited = limit(uri);
        return putToFiler(limited, new StorageObject(null, data), getPath(tenantId, limited));
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        if (isS3Mode()) {
            return s3Storage.put(tenantId, namespace, uri, storageObject);
        }
        URI limited = limit(uri);
        return putToFiler(limited, storageObject, getPath(tenantId, limited));
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, InputStream data) throws IOException {
        if (isS3Mode()) {
            return s3Storage.putInstanceResource(namespace, uri, data);
        }
        URI limited = limit(uri);
        return putToFiler(limited, new StorageObject(null, data), getPath(limited));
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        if (isS3Mode()) {
            return s3Storage.putInstanceResource(namespace, uri, storageObject);
        }
        URI limited = limit(uri);
        return putToFiler(limited, storageObject, getPath(limited));
    }

    private URI putToFiler(URI uri, StorageObject storageObject, String path) throws IOException {
        if (!path.startsWith("/")) path = "/" + path;
        mkdirs(path);
        log.debug("Putting file to SeaweedFS Filer: {}", path);

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

            if (storageObject.metadata() != null && !storageObject.metadata().isEmpty()) {
                storeMetadata(path, storageObject.metadata());
            }
        } catch (Exception e) {
            throw new IOException("Failed to put file in SeaweedFS: " + path, e);
        }

        String uriPath = uri.getPath();
        if (!uriPath.startsWith("/")) uriPath = "/" + uriPath;
        String encodedPath = encodePathForUri(uriPath);

        try {
            return URI.create("kestra://" + encodedPath);
        } catch (Exception e) {
            throw new IOException("Failed to create URI for path: " + uriPath, e);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) throws IOException {
        if (isS3Mode()) {
            return s3Storage.allByPrefix(tenantId, namespace, prefix, includeDirectories);
        }

        String internalStoragePrefix = getPath(tenantId, prefix);
        if (!internalStoragePrefix.startsWith("/")) internalStoragePrefix = "/" + internalStoragePrefix;

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
                        String fullUriPath = uriPrefix + relativePath;
                        if (!fullUriPath.startsWith("/")) fullUriPath = "/" + fullUriPath;
                        if (!fullUriPath.endsWith("/")) fullUriPath = fullUriPath + "/";
                        String encodedPath = encodePathForUri(fullUriPath);
                        try {
                            results.add(URI.create("kestra://" + encodedPath));
                        } catch (Exception e) {
                            log.warn("Failed to create URI for path: {}", uriPrefix + relativePath, e);
                        }
                    }
                    collectAllFiles(fullPath, includeDirectories, results, basePrefix, uriPrefix);
                } else {
                    String relativePath = fullPath.substring(basePrefix.length());
                    String fullUriPath = uriPrefix + relativePath;
                    if (!fullUriPath.startsWith("/")) fullUriPath = "/" + fullUriPath;
                    String encodedPath = encodePathForUri(fullUriPath);
                    try {
                        results.add(URI.create("kestra://" + encodedPath));
                    } catch (Exception e) {
                        log.warn("Failed to create URI for path: {}", uriPrefix + relativePath, e);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to list directory: {}", path, e);
        }
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.list(tenantId, namespace, uri);
        }
        return listFromFiler(getPath(tenantId, uri), tenantId, namespace, uri);
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.listInstanceResource(namespace, uri);
        }
        return listFromFilerInstance(getPath(uri), namespace, uri);
    }

    private List<FileAttributes> listFromFiler(String path, String tenantId, String namespace, URI uri) throws IOException {
        if (path == null || path.isEmpty()) path = "";
        if (!path.startsWith("/")) path = "/" + path;
        if (!path.endsWith("/")) path = path + "/";

        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);
            List<FileAttributes> result = new ArrayList<>();

            for (FilerProto.Entry entry : entries) {
                Map<String, String> metadata = new HashMap<>();
                if (entry.getExtendedCount() > 0) {
                    entry.getExtendedMap().forEach((key, value) ->
                        metadata.put(key, new String(value.toByteArray())));
                }

                result.add(SeaweedFSFileAttributes.builder()
                    .fileName(entry.getName())
                    .size(entry.getAttributes().getFileSize())
                    .lastModifiedTime(entry.getAttributes().getMtime() * 1000)
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

    private List<FileAttributes> listFromFilerInstance(String path, String namespace, URI uri) throws IOException {
        if (path == null || path.isEmpty()) path = "";
        if (!path.startsWith("/")) path = "/" + path;
        if (!path.endsWith("/")) path = path + "/";

        try {
            List<FilerProto.Entry> entries = filerClient.listEntries(path);
            List<FileAttributes> result = new ArrayList<>();

            for (FilerProto.Entry entry : entries) {
                Map<String, String> metadata = new HashMap<>();
                if (entry.getExtendedCount() > 0) {
                    entry.getExtendedMap().forEach((key, value) ->
                        metadata.put(key, new String(value.toByteArray())));
                }

                result.add(SeaweedFSFileAttributes.builder()
                    .fileName(entry.getName())
                    .size(entry.getAttributes().getFileSize())
                    .lastModifiedTime(entry.getAttributes().getMtime() * 1000)
                    .isDirectory(entry.getIsDirectory())
                    .metadata(metadata)
                    .build());
            }

            if (result.isEmpty()) {
                this.getInstanceAttributes(namespace, uri);
            }
            return result;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to list directory in SeaweedFS: " + path, e);
        }
    }

    @Override
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        if (isS3Mode()) {
            return s3Storage.exists(tenantId, namespace, uri);
        }
        try {
            getAttributes(tenantId, namespace, uri);
            return true;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean existsInstanceResource(@Nullable String namespace, URI uri) {
        if (isS3Mode()) {
            return s3Storage.existsInstanceResource(namespace, uri);
        }
        try {
            getInstanceAttributes(namespace, uri);
            return true;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.getAttributes(tenantId, namespace, uri);
        }
        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) path = "/" + path;
        return getFileAttributesFromFiler(path, uri.toString());
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.getInstanceAttributes(namespace, uri);
        }
        String path = getPath(uri);
        if (!path.startsWith("/")) path = "/" + path;
        return getFileAttributesFromFiler(path, uri.toString());
    }

    private FileAttributes getFileAttributesFromFiler(String path, String uriString) throws IOException {
        String[] parts = splitPath(path);

        try {
            FilerProto.Entry entry = filerClient.lookupEntry(parts[0], parts[1]);

            if (entry == null) {
                String markerPath = path.endsWith("/") ? path + ".directory" : path + "/.directory";
                try {
                    FilerProto.Entry markerEntry = filerClient.lookupEntry(
                        markerPath.substring(0, markerPath.lastIndexOf("/")), ".directory");
                    if (markerEntry != null) {
                        return SeaweedFSFileAttributes.builder()
                            .fileName(parts[1])
                            .size(0L)
                            .lastModifiedTime(markerEntry.getAttributes().getMtime() * 1000)
                            .isDirectory(true)
                            .metadata(new HashMap<>())
                            .build();
                    }
                } catch (Exception me) {
                    log.debug("Marker file not found for: {}", path);
                }
                throw new FileNotFoundException(uriString + " (File not found)");
            }

            Map<String, String> metadata = new HashMap<>();
            if (entry.getExtendedCount() > 0) {
                entry.getExtendedMap().forEach((key, value) ->
                    metadata.put(key, new String(value.toByteArray())));
            }

            return SeaweedFSFileAttributes.builder()
                .fileName(entry.getName())
                .size(entry.getAttributes().getFileSize())
                .lastModifiedTime(entry.getAttributes().getMtime() * 1000)
                .isDirectory(entry.getIsDirectory())
                .metadata(metadata)
                .build();
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOT_FOUND")) {
                throw new FileNotFoundException(uriString + " (File not found)");
            }
            throw new IOException("Failed to get attributes from SeaweedFS: " + path, e);
        }
    }

    @Override
    public boolean delete(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.delete(tenantId, namespace, uri);
        }

        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) path = "/" + path;

        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        } catch (Exception e) {
            log.debug("Error getting attributes for {}: {}", path, e.getMessage());
            return false;
        }

        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            URI dirUri = uri.getPath().endsWith("/") ? uri : URI.create(uri + "/");
            List<URI> deleted = deleteByPrefix(tenantId, namespace, dirUri);

            try {
                String markerPath = path.endsWith("/") ? path + ".directory" : path + "/.directory";
                filerClient.rm(markerPath, false, false);
            } catch (Exception e) {
                log.debug("No marker file to delete or deletion failed: {}", path, e);
            }
            return !deleted.isEmpty();
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
        if (isS3Mode()) {
            return s3Storage.deleteInstanceResource(namespace, uri);
        }

        String path = getPath(uri);
        if (!path.startsWith("/")) path = "/" + path;

        FileAttributes fileAttributes;
        try {
            fileAttributes = getInstanceAttributes(namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        } catch (Exception e) {
            log.debug("Error getting attributes for {}: {}", path, e.getMessage());
            return false;
        }

        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            URI dirUri = uri.getPath().endsWith("/") ? uri : URI.create(uri + "/");

            String internalStoragePrefix = getPath(dirUri);
            if (!internalStoragePrefix.startsWith("/")) internalStoragePrefix = "/" + internalStoragePrefix;

            List<URI> allFiles = new ArrayList<>();
            collectAllFiles(internalStoragePrefix, true, allFiles, internalStoragePrefix, dirUri.getPath());

            if (allFiles.isEmpty()) {
                boolean deleted = false;
                try {
                    filerClient.rm(path, false, false);
                    deleted = true;
                } catch (Exception e) {
                    log.debug("Failed to delete empty directory: {}", path, e);
                }

                try {
                    String markerPath = path.endsWith("/") ? path + ".directory" : path + "/.directory";
                    filerClient.rm(markerPath, false, false);
                    deleted = true;
                } catch (Exception e) {
                    log.debug("No marker file to delete: {}", path, e);
                }
                return deleted;
            }

            Collections.reverse(allFiles);
            boolean anyDeleted = false;

            for (URI fileUri : allFiles) {
                try {
                    String filePath = getPath(fileUri);
                    if (!filePath.startsWith("/")) filePath = "/" + filePath;
                    filerClient.rm(filePath, false, false);
                    anyDeleted = true;
                } catch (Exception e) {
                    log.warn("Failed to delete file during directory deletion: {}", fileUri, e);
                }
            }

            try {
                String markerPath = path.endsWith("/") ? path + ".directory" : path + "/.directory";
                filerClient.rm(markerPath, false, false);
            } catch (Exception e) {
                log.debug("No marker file to delete: {}", path, e);
            }
            return anyDeleted;
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
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        if (isS3Mode()) {
            return s3Storage.deleteByPrefix(tenantId, namespace, storagePrefix);
        }

        List<URI> allFiles = allByPrefix(tenantId, namespace, storagePrefix, true);
        List<URI> deletedFiles = new ArrayList<>();

        if (!allFiles.isEmpty()) {
            Collections.reverse(allFiles);

            for (URI fileUri : allFiles) {
                try {
                    String path = getPath(tenantId, fileUri);
                    if (!path.startsWith("/")) path = "/" + path;
                    filerClient.rm(path, false, false);

                    String uriStr = fileUri.toString();
                    if (uriStr.endsWith("/")) {
                        uriStr = uriStr.substring(0, uriStr.length() - 1);
                        deletedFiles.add(URI.create(uriStr));
                    } else {
                        deletedFiles.add(fileUri);
                    }
                } catch (Exception e) {
                    log.warn("Failed to delete file during deleteByPrefix: {}", fileUri, e);
                }
            }
        }

        if (!deletedFiles.isEmpty()) {
            String prefixPath = getPath(tenantId, storagePrefix);
            if (!prefixPath.startsWith("/")) prefixPath = "/" + prefixPath;

            String cleanPrefixPath = prefixPath.endsWith("/") ? prefixPath.substring(0, prefixPath.length() - 1) : prefixPath;

            boolean parentDirectoryDeleted = false;
            try {
                filerClient.rm(cleanPrefixPath, false, false);
                parentDirectoryDeleted = true;
            } catch (Exception e) {
                log.debug("Failed to delete parent directory: {}", cleanPrefixPath, e);
            }

            try {
                filerClient.rm(cleanPrefixPath + "/.directory", false, false);
            } catch (Exception e) {
                log.debug("No marker file to delete: {}", cleanPrefixPath, e);
            }

            if (parentDirectoryDeleted) {
                String uriPath = storagePrefix.getPath();
                if (uriPath.endsWith("/")) uriPath = uriPath.substring(0, uriPath.length() - 1);
                if (!uriPath.startsWith("/")) uriPath = "/" + uriPath;
                String encodedPath = encodePathForUri(uriPath);
                deletedFiles.add(URI.create("kestra://" + encodedPath));
            }
        }
        return deletedFiles;
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.createDirectory(tenantId, namespace, uri);
        }

        validateNoPathTraversal(uri);
        String path = getPath(tenantId, uri);
        if (!path.startsWith("/")) path = "/" + path;
        if (!path.endsWith("/")) path = path + "/";

        try {
            filerClient.mkdirs(path, 0755);
        } catch (Exception e) {
            throw new IOException("Failed to create directory in SeaweedFS: " + path, e);
        }

        String uriPath = uri.getPath();
        if (!uriPath.startsWith("/")) uriPath = "/" + uriPath;
        String encodedPath = encodePathForUri(uriPath);
        try {
            return URI.create("kestra://" + encodedPath);
        } catch (Exception e) {
            throw new IOException("Failed to create URI for path: " + uri.getPath(), e);
        }
    }

    @Override
    public URI createInstanceDirectory(@Nullable String namespace, URI uri) throws IOException {
        if (isS3Mode()) {
            return s3Storage.createInstanceDirectory(namespace, uri);
        }

        validateNoPathTraversal(uri);
        String path = getPath(uri);
        if (!path.startsWith("/")) path = "/" + path;
        if (!path.endsWith("/")) path = path + "/";

        try {
            filerClient.mkdirs(path, 0755);

            String markerPath = path + ".directory";
            try (SeaweedOutputStream out = new SeaweedOutputStream(filerClient, markerPath)) {
                out.write(new byte[0]);
            } catch (Exception e) {
                log.debug("Failed to create directory marker: {}", markerPath, e);
            }
        } catch (Exception e) {
            throw new IOException("Failed to create directory in SeaweedFS: " + path, e);
        }

        String uriPath = uri.getPath();
        if (!uriPath.startsWith("/")) uriPath = "/" + uriPath;
        String encodedPath = encodePathForUri(uriPath);
        try {
            return URI.create("kestra://" + encodedPath);
        } catch (Exception e) {
            throw new IOException("Failed to create URI for path: " + uri.getPath(), e);
        }
    }

    @Override
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        if (isS3Mode()) {
            return s3Storage.move(tenantId, namespace, from, to);
        }

        String sourcePath = getPath(tenantId, from);
        String destPath = getPath(tenantId, to);
        if (!sourcePath.startsWith("/")) sourcePath = "/" + sourcePath;
        if (!destPath.startsWith("/")) destPath = "/" + destPath;

        try {
            FileAttributes attributes = getAttributes(tenantId, namespace, from);

            if (attributes.getType() == FileAttributes.FileType.Directory) {
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

        String toPath = to.getPath();
        if (!toPath.startsWith("/")) toPath = "/" + toPath;
        String encodedPath = encodePathForUri(toPath);
        try {
            return URI.create("kestra://" + encodedPath);
        } catch (Exception e) {
            throw new IOException("Failed to create URI for path: " + to.getPath(), e);
        }
    }

    private void moveSingleFile(String source, String dest) throws IOException {
        if (!source.startsWith("/")) source = "/" + source;
        if (!dest.startsWith("/")) dest = "/" + dest;

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