package io.kestra.storage.seaweedfs;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@KestraTest
class SeaweedFSStorageSchemaTest {
    @Inject
    private JsonSchemaGenerator jsonSchemaGenerator;

    @SuppressWarnings("unchecked")
    @Test
    void schema() {
        var schema = jsonSchemaGenerator.properties(StorageInterface.class, SeaweedFSStorage.class);

        assertThat((List<String>) schema.get("required"), containsInAnyOrder("filerHost"));
    }
}
