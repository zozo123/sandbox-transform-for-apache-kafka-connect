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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/** Emits a fixed run of payment records, one of them flagged internal. */
public final class PaymentsSourceConnector extends SourceConnector {

    /** Set per run so a reused broker cannot feed a test stale records from an earlier run. */
    public static final String TOPIC_CONFIG = "sandbox.it.topic";

    private String topic;
    static final int RECORDS = 5;
    static final String CARD = "4111111111111111";
    static final String MASKED = "************1111";

    @Override
    public void start(final Map<String, String> props) {
        this.topic = props.get(TOPIC_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PaymentsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put(TOPIC_CONFIG, topic);
        return Collections.singletonList(taskConfig);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return "test";
    }

    public static final class PaymentsSourceTask extends SourceTask {

        private String topic;

        private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .field("card", Schema.STRING_SCHEMA)
            .field("amount", Schema.INT32_SCHEMA)
            .field("internal", Schema.BOOLEAN_SCHEMA)
            .build();

        private int produced;

        @Override
        public void start(final Map<String, String> props) {
            this.topic = props.get(TOPIC_CONFIG);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (produced >= RECORDS) {
                Thread.sleep(200);
                return null;
            }
            final List<SourceRecord> batch = new ArrayList<>();
            final Map<String, String> partition = Collections.singletonMap("p", "0");
            final Map<String, String> offset = Collections.singletonMap("o", String.valueOf(produced));
            // The last record is flagged internal, so the guest should drop it.
            final boolean internal = produced == RECORDS - 1;
            final Struct value = new Struct(VALUE_SCHEMA)
                .put("card", CARD)
                .put("amount", 1299)
                .put("internal", internal);
            batch.add(new SourceRecord(partition, offset, topic, VALUE_SCHEMA, value));
            produced++;
            return batch;
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return "test";
        }
    }
}
