package io.kestra.storage.seaweedfs;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.storages.FileAttributes;
import io.kestra.core.utils.IdUtils;

import seaweedfs.client.FilerClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeaweedFSStoragePrefixTest {

    private static final String PREFIX = "kestra-prefix-test";

    private SeaweedFSStorage storage;
    // Raw client, to assert where entries really land on the filer.
    private FilerClient raw;

    @BeforeAll
    void setup() {
        SeaweedFSTestContainers.start();

        storage = storage(PREFIX + "/");
        storage.init();
        raw = new FilerClient(SeaweedFSTestContainers.getFilerHost(), SeaweedFSTestContainers.getFilerGrpcPort());
    }

    private static SeaweedFSStorage storage(String prefix) {
        return SeaweedFSStorage.builder()
            .filerHost(SeaweedFSTestContainers.getFilerHost())
            .filerPort(SeaweedFSTestContainers.getFilerGrpcPort())
            .prefix(prefix)
            .replication("000")
            .build();
    }

    /** True if an entry exists at the given absolute filer path. */
    private boolean existsOnFiler(String absolutePath) {
        int slash = absolutePath.lastIndexOf('/');
        String dir = slash == 0 ? "/" : absolutePath.substring(0, slash);
        return raw.lookupEntry(dir, absolutePath.substring(slash + 1)) != null;
    }

    private static ByteArrayInputStream data(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("put/get/exists/getAttributes/getWithMetadata store tenant data under the prefix")
    void tenantFileRoundTrip() throws Exception {
        String tenant = IdUtils.create();
        URI uri = URI.create("/ns/file.txt");

        URI returned = storage.put(tenant, "ns", uri, data("hello"));

        assertThat(returned.toString(), is("kestra:///ns/file.txt"));
        assertTrue(existsOnFiler("/" + PREFIX + "/" + tenant + "/ns/file.txt"));
        assertFalse(existsOnFiler("/" + tenant + "/ns/file.txt"));
        assertFalse(existsOnFiler("/" + tenant));

        assertThat(new String(storage.get(tenant, "ns", uri).readAllBytes(), StandardCharsets.UTF_8), is("hello"));
        assertTrue(storage.exists(tenant, "ns", uri));
        assertThat(storage.getAttributes(tenant, "ns", uri).getSize(), is(5L));
        assertThat(new String(storage.getWithMetadata(tenant, "ns", uri).inputStream().readAllBytes(), StandardCharsets.UTF_8), is("hello"));
    }

    @Test
    @DisplayName("list and allByPrefix return URIs without the prefix")
    void listAndAllByPrefix() throws Exception {
        String tenant = IdUtils.create();
        storage.put(tenant, "ns", URI.create("/ns/dir/a.txt"), data("a"));
        storage.put(tenant, "ns", URI.create("/ns/dir/sub/b.txt"), data("b"));

        List<FileAttributes> listed = storage.list(tenant, "ns", URI.create("/ns/dir"));
        assertThat(listed.stream().map(FileAttributes::getFileName).toList(), containsInAnyOrder("a.txt", "sub"));

        List<URI> files = storage.allByPrefix(tenant, "ns", URI.create("/ns/dir/"), false);
        assertThat(files, containsInAnyOrder(URI.create("kestra:///ns/dir/a.txt"), URI.create("kestra:///ns/dir/sub/b.txt")));

        List<URI> withDirs = storage.allByPrefix(tenant, "ns", URI.create("/ns/dir/"), true);
        assertThat(withDirs, hasItem(URI.create("kestra:///ns/dir/sub/")));
        withDirs.forEach(u -> assertThat(u.toString(), not(containsString(PREFIX))));
    }

    @Test
    @DisplayName("createDirectory, move (file and directory) and delete stay under the prefix")
    void createMoveDelete() throws Exception {
        String tenant = IdUtils.create();
        String root = "/" + PREFIX + "/" + tenant;

        assertThat(storage.createDirectory(tenant, "ns", URI.create("/ns/emptydir")).toString(), is("kestra:///ns/emptydir"));
        assertTrue(existsOnFiler(root + "/ns/emptydir"));

        storage.put(tenant, "ns", URI.create("/ns/src.txt"), data("x"));
        URI moved = storage.move(tenant, "ns", URI.create("/ns/src.txt"), URI.create("/ns/dst.txt"));
        assertThat(moved.toString(), is("kestra:///ns/dst.txt"));
        assertTrue(existsOnFiler(root + "/ns/dst.txt"));
        assertFalse(existsOnFiler(root + "/ns/src.txt"));

        storage.put(tenant, "ns", URI.create("/ns/d1/f.txt"), data("y"));
        storage.move(tenant, "ns", URI.create("/ns/d1"), URI.create("/ns/d2"));
        assertTrue(existsOnFiler(root + "/ns/d2/f.txt"));
        assertFalse(existsOnFiler("/" + tenant));

        assertTrue(storage.delete(tenant, "ns", URI.create("/ns/dst.txt")));
        assertFalse(existsOnFiler(root + "/ns/dst.txt"));

        List<URI> deleted = storage.deleteByPrefix(tenant, "ns", URI.create("/ns/d2/"));
        assertThat(deleted, hasItem(URI.create("kestra:///ns/d2/f.txt")));
        deleted.forEach(u -> assertThat(u.toString(), not(containsString(PREFIX))));
        assertFalse(existsOnFiler(root + "/ns/d2/f.txt"));
    }

    @Test
    @DisplayName("instance resources are stored under <prefix>/<path>, without tenant")
    void instanceResources() throws Exception {
        String name = IdUtils.create();
        URI uri = URI.create("/instance-" + name + "/file.txt");

        URI returned = storage.putInstanceResource("ns", uri, data("inst"));

        assertThat(returned.toString(), is("kestra://" + uri.getPath()));
        assertTrue(existsOnFiler("/" + PREFIX + "/instance-" + name + "/file.txt"));
        assertFalse(existsOnFiler("/instance-" + name + "/file.txt"));
        assertThat(new String(storage.getInstanceResource("ns", uri).readAllBytes(), StandardCharsets.UTF_8), is("inst"));
        assertTrue(storage.existsInstanceResource("ns", uri));
        assertThat(storage.listInstanceResource("ns", URI.create("/instance-" + name)).size(), is(1));

        assertTrue(storage.deleteInstanceResource("ns", uri));
        assertFalse(storage.existsInstanceResource("ns", uri));
    }

    @Test
    @DisplayName("prefix spellings are normalised to the same layout")
    void prefixNormalisation() {
        for (String prefix : new String[]{"kestra-prefix-test", "/kestra-prefix-test", "kestra-prefix-test/", "/kestra-prefix-test//", " /kestra-prefix-test/ "}) {
            SeaweedFSStorage s = storage(prefix);

            assertThat(prefix, s.getPath("t", URI.create("/ns/f.txt")), is("kestra-prefix-test/t/ns/f.txt"));
            assertThat(prefix, s.getPath(URI.create("/ns/f.txt")), is("kestra-prefix-test/ns/f.txt"));
        }
    }

    @Test
    @DisplayName("nested prefix and empty/null prefix")
    void nestedAndEmptyPrefix() {
        assertThat(storage("a//b/").getPath("t", URI.create("/f")), is("a/b/t/f"));

        for (String empty : new String[]{"", "  ", "/", null}) {
            SeaweedFSStorage s = storage(empty);
            assertThat(s.getPath("t", URI.create("/ns/f.txt")), is("t/ns/f.txt"));
            assertThat(s.getPath(URI.create("/ns/f.txt")), is("ns/f.txt"));
        }
    }

    @Test
    @DisplayName("data written through the S3 gateway layout (/buckets/<bucket>/<tenant>/...) is readable")
    void s3GatewayLayout() throws Exception {
        String tenant = IdUtils.create();
        String bucket = "bucket-" + IdUtils.create();
        // Simulate a bucket copied through the SeaweedFS S3 gateway.
        raw.mkdirs("/buckets/" + bucket + "/" + tenant + "/ns", 0755);
        try (var out = new seaweedfs.client.SeaweedOutputStream(raw, "/buckets/" + bucket + "/" + tenant + "/ns/gw.txt")) {
            out.write("from-gateway".getBytes(StandardCharsets.UTF_8));
        }

        SeaweedFSStorage s = storage("/buckets/" + bucket + "/");
        s.init();

        URI uri = URI.create("/ns/gw.txt");
        assertThat(new String(s.get(tenant, "ns", uri).readAllBytes(), StandardCharsets.UTF_8), is("from-gateway"));
        assertThat(s.list(tenant, "ns", URI.create("/ns")).stream().map(FileAttributes::getFileName).toList(), contains("gw.txt"));
        assertThrows(FileNotFoundException.class, () -> storage.get(tenant, "ns", uri));
    }

    @Test
    @DisplayName("init() rejects a prefix with '..' segments")
    void initRejectsParentTraversal() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> storage("a/../b").init());
        assertThat(e.getMessage(), containsString("'..' segments are not allowed"));
    }
}
