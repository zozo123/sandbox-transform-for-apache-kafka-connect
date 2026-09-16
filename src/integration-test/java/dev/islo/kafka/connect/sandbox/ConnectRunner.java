/*
 * Copyright 2026 Yossi Eliaz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.islo.kafka.connect.sandbox;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;

/**
 * Starts a real Connect worker in-process against a real broker.
 *
 * <p>Adapted from the equivalent harness in Aiven's
 * transforms-for-apache-kafka-connect (Apache-2.0).
 */
final class ConnectRunner {

    private final File pluginDir;
    private final String bootstrapServers;

    private Herder herder;
    private Connect connect;

    ConnectRunner(final File pluginDir, final String bootstrapServers) {
        this.pluginDir = pluginDir;
        this.bootstrapServers = bootstrapServers;
    }

    void start() {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", bootstrapServers);
        workerProps.put("offset.flush.interval.ms", "1000");
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "");
        workerProps.put("listeners", "HTTP://localhost:0");
        // The transform is discovered the way an operator would install it: as a plugin
        // directory, loaded by Connect's own plugin classloader.
        workerProps.put("plugin.path", pluginDir.getPath());

        final Plugins plugins = new Plugins(workerProps);
        final StandaloneConfig config = new StandaloneConfig(workerProps);
        final ConnectorClientConfigOverridePolicy overridePolicy =
            new NoneConnectorClientConfigOverridePolicy();

        final Worker worker = new Worker(
            "sandbox-it-worker", Time.SYSTEM, plugins, config,
            new MemoryOffsetBackingStore() {
                @Override
                public Set<Map<String, Object>> connectorPartitions(final String connectorName) {
                    return Collections.emptySet();
                }
            },
            overridePolicy);
        herder = new StandaloneHerder(worker, "sandbox-it-cluster", overridePolicy);

        final RestClient restClient = new RestClient(config);
        final ConnectRestServer rest =
            new ConnectRestServer(config.rebalanceTimeout(), restClient, config.originals());
        rest.initializeServer();

        connect = new Connect(herder, rest);
        connect.start();
    }

    void createConnector(final Map<String, String> config)
        throws ExecutionException, InterruptedException {
        final FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>();
        herder.putConnectorConfig(config.get(ConnectorConfig.NAME_CONFIG), config, false, callback);
        callback.get();
    }

    void stop() {
        if (connect != null) {
            connect.stop();
            connect.awaitStop();
        }
    }
}
