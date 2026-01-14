package io.kestra.storage.seaweedfs;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeaweedFSStorageTest extends StorageTestSuite {

    static {
        SeaweedFSTestContainers.start();

        System.setProperty("seaweedfs.filer.host", SeaweedFSTestContainers.getFilerHost());
        System.setProperty("seaweedfs.filer.port", String.valueOf(SeaweedFSTestContainers.getFilerGrpcPort()));
    }

    @Inject
    StorageInterface storageInterface;

    private static final int LARGE_FILE_SIZE = 15 * 1024 * 1024; // 15MB
    private static final int VERY_LARGE_FILE_SIZE = 20 * 1024 * 1024; // 20MB

    @BeforeAll
    void setup() {

        if (storageInterface instanceof SeaweedFSStorage) {
            SeaweedFSStorage storage = (SeaweedFSStorage) storageInterface;
        }
    }

    @AfterAll
    void teardown() {
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        System.out.println("\n>>> Running: " + testInfo.getDisplayName());
    }

    @AfterEach
    void afterEach(TestInfo testInfo) {
        System.out.println("<<< Completed: " + testInfo.getDisplayName());
    }

    @Test
    @DisplayName("Test large file upload and download (15MB)")
    void testLargeFileHandling() throws Exception {
        String fileName = "test-large-file-" + IdUtils.create() + ".bin";
        URI fileUri = URI.create("/" + fileName);

        byte[] largeContent = generateRandomBytes(LARGE_FILE_SIZE);
        String originalChecksum = calculateMD5(largeContent);

        long uploadStart = System.currentTimeMillis();
        InputStream inputStream = new ByteArrayInputStream(largeContent);
        URI resultUri = storageInterface.put(TenantService.MAIN_TENANT, null, fileUri, inputStream);
        long uploadTime = System.currentTimeMillis() - uploadStart;

        assertNotNull(resultUri, "Upload should return a URI");
        assertTrue(storageInterface.exists(TenantService.MAIN_TENANT, null, fileUri),
            "Large file should exist after upload");

        long downloadStart = System.currentTimeMillis();
        InputStream downloadStream = storageInterface.get(TenantService.MAIN_TENANT, null, fileUri);
        byte[] downloadedContent = downloadStream.readAllBytes();
        downloadStream.close();
        long downloadTime = System.currentTimeMillis() - downloadStart;

        assertEquals(LARGE_FILE_SIZE, downloadedContent.length,
            "Downloaded file size should match uploaded size");

        String downloadedChecksum = calculateMD5(downloadedContent);

        assertEquals(originalChecksum, downloadedChecksum,
            "Downloaded large file checksum should match original - data integrity verified");

        storageInterface.delete(TenantService.MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Test streaming large file (20MB)")
    void testVeryLargeFileStreaming() throws Exception {
        String fileName = "test-streaming-large-" + IdUtils.create() + ".bin";
        URI fileUri = URI.create("/" + fileName);

        long uploadStart = System.currentTimeMillis();
        InputStream largeInputStream = new RandomInputStream(VERY_LARGE_FILE_SIZE, 42L);
        URI resultUri = storageInterface.put(TenantService.MAIN_TENANT, null, fileUri, largeInputStream);
        long uploadTime = System.currentTimeMillis() - uploadStart;

        assertNotNull(resultUri, "Streaming upload should return a URI");

        long downloadStart = System.currentTimeMillis();
        InputStream downloadStream = storageInterface.get(TenantService.MAIN_TENANT, null, fileUri);
        long totalBytesRead = 0;
        byte[] buffer = new byte[8192];
        int bytesRead;

        while ((bytesRead = downloadStream.read(buffer)) != -1) {
            totalBytesRead += bytesRead;
        }
        downloadStream.close();
        long downloadTime = System.currentTimeMillis() - downloadStart;

        assertEquals(VERY_LARGE_FILE_SIZE, totalBytesRead,
            "Streamed file size should match expected size");

        storageInterface.delete(TenantService.MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Test binary data integrity")
    void testBinaryDataIntegrity() throws Exception {
        String fileName = "test-binary-" + IdUtils.create() + ".bin";
        URI fileUri = URI.create("/" + fileName);

        byte[] binaryContent = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryContent[i] = (byte) i;
        }

        storageInterface.put(TenantService.MAIN_TENANT, null, fileUri,
            new ByteArrayInputStream(binaryContent));

        InputStream downloadStream = storageInterface.get(TenantService.MAIN_TENANT, null, fileUri);
        byte[] downloadedContent = downloadStream.readAllBytes();
        downloadStream.close();

        assertArrayEquals(binaryContent, downloadedContent,
            "Binary data should be identical after upload/download");

        // Cleanup
        storageInterface.delete(TenantService.MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Test deletion of multiple files by prefix")
    void testDeleteMultipleFilesByPrefix() throws Exception {
        String prefix = "delete-test-" + IdUtils.create();
        int fileCount = 10;

        for (int i = 0; i < fileCount; i++) {
            URI fileUri = URI.create("/" + prefix + "/file-" + i + ".txt");
            String content = "Content of file " + i;
            storageInterface.put(TenantService.MAIN_TENANT, null, fileUri,
                new ByteArrayInputStream(content.getBytes()));
        }

        URI prefixUri = URI.create("/" + prefix + "/");
        var filesBefore = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, prefixUri, false);
        long actualFileCount = filesBefore.size();

        assertTrue(actualFileCount >= fileCount,
            "Should have at least " + fileCount + " files before deletion");

        var deletedUris = storageInterface.deleteByPrefix(TenantService.MAIN_TENANT, null, prefixUri);

        var filesAfter = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, prefixUri, false);
        long remainingFileCount = filesAfter.size();

        assertEquals(0, remainingFileCount, "All files should be deleted");
    }

    @Test
    @DisplayName("Test listing files in nested directories")
    void testListNestedDirectories() throws Exception {
        String basePrefix = "list-nested-" + IdUtils.create();

        String[] filePaths = {
            "/" + basePrefix + "/root-file.txt",
            "/" + basePrefix + "/dir1/file1.txt",
            "/" + basePrefix + "/dir1/file2.txt",
            "/" + basePrefix + "/dir2/file3.txt",
            "/" + basePrefix + "/dir2/subdir/file4.txt"
        };

        for (String path : filePaths) {
            storageInterface.put(TenantService.MAIN_TENANT, null, URI.create(path),
                new ByteArrayInputStream(("Content: " + path).getBytes()));
        }

        URI baseUri = URI.create("/" + basePrefix + "/");
        var allUris = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, baseUri, false);

        long uriCount = allUris.size();

        assertTrue(uriCount >= filePaths.length,
            "Should list at least " + filePaths.length + " URIs recursively");

        // Cleanup
        storageInterface.deleteByPrefix(TenantService.MAIN_TENANT, null, baseUri);
    }

    @Test
    @DisplayName("Test listing with pagination (100+ files)")
    void testListingPagination() throws Exception {
        String prefix = "pagination-test-" + IdUtils.create();
        int fileCount = 150;

        for (int i = 0; i < fileCount; i++) {
            URI fileUri = URI.create("/" + prefix + "/file-" + String.format("%04d", i) + ".txt");
            storageInterface.put(TenantService.MAIN_TENANT, null, fileUri,
                new ByteArrayInputStream(("Content " + i).getBytes()));
        }

        URI prefixUri = URI.create("/" + prefix + "/");
        var allUris = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, prefixUri, false);

        long actualUriCount = allUris.size();

        assertEquals(fileCount, actualUriCount,
            "Should list all " + fileCount + " URIs with pagination");

        // Cleanup
        storageInterface.deleteByPrefix(TenantService.MAIN_TENANT, null, prefixUri);
    }

    @Test
    @DisplayName("Test concurrent file uploads")
    void testConcurrentUploads() throws Exception {
        String prefix = "concurrent-" + IdUtils.create();
        int numThreads = 10;

        Thread[] threads = new Thread[numThreads];
        Exception[] exceptions = new Exception[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadNum = i;
            threads[i] = new Thread(() -> {
                try {
                    URI fileUri = URI.create("/" + prefix + "/file-" + threadNum + ".txt");
                    String content = "Concurrent content from thread " + threadNum;
                    storageInterface.put(TenantService.MAIN_TENANT, null, fileUri,
                        new ByteArrayInputStream(content.getBytes()));
                } catch (Exception e) {
                    exceptions[threadNum] = e;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (int i = 0; i < numThreads; i++) {
            assertNull(exceptions[i], "Thread " + i + " should not throw exception: " +
                (exceptions[i] != null ? exceptions[i].getMessage() : ""));
        }

        // Verify all files were uploaded
        URI prefixUri = URI.create("/" + prefix + "/");
        var uris = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, prefixUri, false);

        long uriCount = uris.size();

        assertEquals(numThreads, uriCount, "All concurrent uploads should succeed");
        // Cleanup
        storageInterface.deleteByPrefix(TenantService.MAIN_TENANT, null, prefixUri);
    }

    @Test
    @DisplayName("Test moving large file")
    void testMoveLargeFile() throws Exception {
        String sourceFile = "move-source-" + IdUtils.create() + ".bin";
        String destFile = "move-dest-" + IdUtils.create() + ".bin";
        URI sourceUri = URI.create("/" + sourceFile);
        URI destUri = URI.create("/" + destFile);

        int moveFileSize = 5 * 1024 * 1024; // 5MB

        byte[] content = generateRandomBytes(moveFileSize);
        String originalChecksum = calculateMD5(content);

        storageInterface.put(TenantService.MAIN_TENANT, null, sourceUri,
            new ByteArrayInputStream(content));

        URI resultUri = storageInterface.move(TenantService.MAIN_TENANT, null, sourceUri, destUri);
        assertNotNull(resultUri, "Move should return destination URI");

        // Verify source no longer exists
        assertFalse(storageInterface.exists(TenantService.MAIN_TENANT, null, sourceUri),
            "Source file should not exist after move");

        // Verify destination exists and content is intact
        assertTrue(storageInterface.exists(TenantService.MAIN_TENANT, null, destUri),
            "Destination file should exist after move");

        InputStream downloadStream = storageInterface.get(TenantService.MAIN_TENANT, null, destUri);
        byte[] movedContent = downloadStream.readAllBytes();
        downloadStream.close();

        String movedChecksum = calculateMD5(movedContent);
        assertEquals(originalChecksum, movedChecksum,
            "Moved file content should match original");
        // Cleanup
        storageInterface.delete(TenantService.MAIN_TENANT, null, destUri);
    }

    private byte[] generateRandomBytes(int size) {
        byte[] content = new byte[size];
        Random random = new Random(123);
        random.nextBytes(content);
        return content;
    }

    private String calculateMD5(byte[] content) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(content);

        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static class RandomInputStream extends InputStream {
        private final long size;
        private long position = 0;
        private final Random random;

        public RandomInputStream(long size, long seed) {
            this.size = size;
            this.random = new Random(seed);
        }

        @Override
        public int read() {
            if (position >= size) {
                return -1;
            }
            position++;
            return random.nextInt(256);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (position >= size) {
                return -1;
            }

            int bytesToRead = (int) Math.min(len, size - position);
            for (int i = 0; i < bytesToRead; i++) {
                b[off + i] = (byte) random.nextInt(256);
            }
            position += bytesToRead;
            return bytesToRead;
        }
    }
}