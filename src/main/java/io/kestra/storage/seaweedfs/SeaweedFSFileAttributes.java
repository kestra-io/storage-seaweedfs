package io.kestra.storage.seaweedfs;

import io.kestra.core.storages.FileAttributes;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class SeaweedFSFileAttributes implements FileAttributes {
    String fileName;
    Long size;
    Long lastModifiedTime;
    boolean isDirectory;
    Map<String, String> metadata;

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public FileAttributes.FileType getType() {
        return isDirectory ? FileType.Directory : FileType.File;
    }

    @Override
    public long getLastModifiedTime() {
        return lastModifiedTime != null ? lastModifiedTime : 0L;
    }

    @Override
    public long getCreationTime() {
        return getLastModifiedTime();
    }

    @Override
    public long getSize() {
        return size != null ? size : 0L;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }
}
