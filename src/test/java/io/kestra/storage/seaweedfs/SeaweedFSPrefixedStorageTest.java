package io.kestra.storage.seaweedfs;

import java.net.URI;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Property(name = "kestra.storage.seaweedfs.prefix", value = "/suite-prefix/nested/")
@ExtendWith(SeaweedFSPrefixedStorageTest.SkipUnprefixedGetPath.class)
class SeaweedFSPrefixedStorageTest extends StorageTestSuite {

    static {
        SeaweedFSTestContainers.start();

        System.setProperty("seaweedfs.filer.host", SeaweedFSTestContainers.getFilerHost());
        System.setProperty("seaweedfs.filer.port", String.valueOf(SeaweedFSTestContainers.getFilerGrpcPort()));
    }

    @Inject
    StorageInterface storageInterface;

    @Test
    void configuredPrefixIsApplied() {
        var storage = (SeaweedFSStorage) storageInterface;

        assertEquals("suite-prefix/nested/folder1", storage.getPath(URI.create("/folder1")));
        assertEquals("suite-prefix/nested/tenant/folder1", storage.getPath("tenant", URI.create("/folder1")));
    }

    // The core getPath() test hard-codes an empty prefix and is package-private, so it cannot be overridden here.
    static class SkipUnprefixedGetPath implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            return context.getTestMethod().filter(m -> m.getName().equals("getPath") && m.getDeclaringClass() == StorageTestSuite.class).isPresent()
                ? disabled("core getPath() assumes no prefix; covered by configuredPrefixIsApplied")
                : enabled("");
        }
    }
}
