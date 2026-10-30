package io.kestra.storage.seaweedfs;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;

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
        var schema = jsonSchemaGenerator.schemas(SeaweedFSStorage.class, false);

        var definition = (Map<String, Object>) ((Map<String, Object>) schema.get("definitions")).get(SeaweedFSStorage.class.getName());
        assertThat((List<String>) definition.get("required"), containsInAnyOrder("filerHost"));
    }
}
