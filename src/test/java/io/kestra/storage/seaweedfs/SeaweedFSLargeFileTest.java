package io.kestra.storage.seaweedfs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Random;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;


@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeaweedFSLargeFileTest {

    private SeaweedFSStorage storage;

    private static final int MEGA_BYTE = 1024 * 1024;

    @BeforeAll
    void setup() {
        // Start SeaweedFS containers before all tests
        SeaweedFSTestContainers.start();

        storage = SeaweedFSStorage.builder()
            .filerHost(SeaweedFSTestContainers.getFilerHost())
            .filerPort(SeaweedFSTestContainers.getFilerGrpcPort())
            .prefix("kestra-large-test/")
            .replication("000")
            .build();

        storage.init();

    }

    @Test
    @DisplayName("Upload and download exactly 10MB file")
    void testExactly10MBFile() throws Exception {
        int fileSize = 10 * MEGA_BYTE;
        String fileName = IdUtils.create() + "-10mb.bin";
        URI fileUri = URI.create("/" + fileName);


        byte[] content = new byte[fileSize];
        Random random = new Random(999);
        random.nextBytes(content);

        String originalChecksum = calculateMD5(content);

        long uploadStart = System.currentTimeMillis();
        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content));
        long uploadDuration = System.currentTimeMillis() - uploadStart;


        // Download and verify
        long downloadStart = System.currentTimeMillis();
        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            byte[] downloaded = is.readAllBytes();

            long downloadDuration = System.currentTimeMillis() - downloadStart;

            String downloadedChecksum = calculateMD5(downloaded);

            assertEquals(fileSize, downloaded.length, "File size should be exactly 10MB");
            assertEquals(originalChecksum, downloadedChecksum, "Content should match");
        }

        storage.delete(MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Upload and download 25MB file")
    void test25MBFile() throws Exception {
        int fileSize = 25 * MEGA_BYTE;
        String fileName = IdUtils.create() + "-25mb.bin";
        URI fileUri = URI.create("/" + fileName);


        // Create file
        byte[] content = new byte[fileSize];
        Random random = new Random(777);
        random.nextBytes(content);

        String originalChecksum = calculateMD5(content);

        long uploadStart = System.currentTimeMillis();
        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content));
        long uploadDuration = System.currentTimeMillis() - uploadStart;


        long downloadStart = System.currentTimeMillis();
        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            byte[] downloaded = is.readAllBytes();

            long downloadDuration = System.currentTimeMillis() - downloadStart;

            String downloadedChecksum = calculateMD5(downloaded);

            assertEquals(fileSize, downloaded.length, "File size should be exactly 25MB");
            assertEquals(originalChecksum, downloadedChecksum, "Content should match");
        }
        storage.delete(MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Upload multiple large files sequentially")
    void testMultipleLargeFilesSequential() throws Exception {
        int fileSize = 12 * MEGA_BYTE;
        int numFiles = 5;
        String prefix = IdUtils.create() + "-seq";


        long totalUploadTime = 0;
        long totalDownloadTime = 0;

        for (int i = 0; i < numFiles; i++) {
            String fileName = prefix + "-file-" + i + ".bin";
            URI fileUri = URI.create("/" + fileName);

            byte[] content = new byte[fileSize];
            new Random(i).nextBytes(content);
            String checksum = calculateMD5(content);

            long uploadStart = System.currentTimeMillis();
            storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content));
            totalUploadTime += System.currentTimeMillis() - uploadStart;

            long downloadStart = System.currentTimeMillis();
            try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
                byte[] downloaded = is.readAllBytes();
                totalDownloadTime += System.currentTimeMillis() - downloadStart;

                String downloadedChecksum = calculateMD5(downloaded);
                assertEquals(checksum, downloadedChecksum, "File " + i + " content should match");
            }
            storage.delete(MAIN_TENANT, null, fileUri);
        }
    }

    @Test
    @DisplayName("Test file with various chunk sizes")
    void testVariousChunkSizes() throws Exception {
        int[] chunkSizes = {
            1 * MEGA_BYTE,
            5 * MEGA_BYTE,
            10 * MEGA_BYTE,
            15 * MEGA_BYTE,
            20 * MEGA_BYTE
        };


        for (int size : chunkSizes) {
            String fileName = IdUtils.create() + "-" + (size / MEGA_BYTE) + "mb.bin";
            URI fileUri = URI.create("/" + fileName);

            byte[] content = new byte[size];
            new Random(size).nextBytes(content);
            String checksum = calculateMD5(content);

            storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content));

            try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
                byte[] downloaded = is.readAllBytes();
                String downloadedChecksum = calculateMD5(downloaded);

                assertEquals(size, downloaded.length,
                    "Size mismatch for " + (size / MEGA_BYTE) + "MB file");
                assertEquals(checksum, downloadedChecksum,
                    "Checksum mismatch for " + (size / MEGA_BYTE) + "MB file");

                System.out.println("  ✓ " + (size / MEGA_BYTE) + "MB file passed");
            }
            storage.delete(MAIN_TENANT, null, fileUri);
        }
    }

    @Test
    @DisplayName("Test streaming read of large file")
    void testStreamingRead() throws Exception {
        int fileSize = 15 * MEGA_BYTE;
        String fileName = IdUtils.create() + "-streaming.bin";
        URI fileUri = URI.create("/" + fileName);

        System.out.println("\nTest: Streaming read of 15MB file");

        byte[] content = new byte[fileSize];
        new Random(555).nextBytes(content);
        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content));

        int chunkSize = 4096;
        long totalRead = 0;
        int chunks = 0;

        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;

            while ((bytesRead = is.read(buffer)) != -1) {
                totalRead += bytesRead;
                chunks++;
            }
        }

        assertEquals(fileSize, totalRead, "Should read entire file via streaming");
        storage.delete(MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Test empty file")
    void testEmptyFile() throws Exception {
        String fileName = IdUtils.create() + "-empty.txt";
        URI fileUri = URI.create("/" + fileName);

        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(new byte[0]));

        assertTrue(storage.exists(MAIN_TENANT, null, fileUri), "Empty file should exist");

        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            byte[] downloaded = is.readAllBytes();
            assertEquals(0, downloaded.length, "Empty file should have 0 bytes");
        }
        storage.delete(MAIN_TENANT, null, fileUri);
    }

    @Test
    @DisplayName("Test file with special characters in name")
    void testSpecialCharactersFileName() throws Exception {
        String[] specialNames = {
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
            "file (with) parens.txt"
        };

        for (String name : specialNames) {
            String fileName = IdUtils.create() + "-" + name;
            String encodedName = fileName.replace(" ", "%20").replace("(", "%28").replace(")", "%29");
            URI fileUri = URI.create("/" + encodedName);

            String content = "Content for: " + name;

            storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content.getBytes()));

            try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
                String downloaded = new String(is.readAllBytes());
                assertEquals(content, downloaded, "Content should match for: " + name);
            }
            storage.delete(MAIN_TENANT, null, fileUri);
        }
    }

    @Test
    @DisplayName("Test delete and re-upload same file")
    void testDeleteAndReupload() throws Exception {
        String fileName = IdUtils.create() + "-reupload.txt";
        URI fileUri = URI.create("/" + fileName);

        String content1 = "First version";
        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content1.getBytes()));

        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            assertThat(new String(is.readAllBytes()), is(content1));
        }

        boolean deleted = storage.delete(MAIN_TENANT, null, fileUri);
        assertTrue(deleted, "Delete should succeed");
        assertFalse(storage.exists(MAIN_TENANT, null, fileUri), "File should not exist after delete");

        String content2 = "Second version";
        storage.put(MAIN_TENANT, null, fileUri, new ByteArrayInputStream(content2.getBytes()));

        try (InputStream is = storage.get(MAIN_TENANT, null, fileUri)) {
            assertThat(new String(is.readAllBytes()), is(content2));
        }

        storage.delete(MAIN_TENANT, null, fileUri);
    }

    private String calculateMD5(byte[] content) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(content);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}