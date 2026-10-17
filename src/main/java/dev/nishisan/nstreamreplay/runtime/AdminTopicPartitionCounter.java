package dev.nishisan.nstreamreplay.runtime;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** {@link TopicPartitionCounter} de produção, sobre o {@link Admin} do Kafka. */
public final class AdminTopicPartitionCounter implements TopicPartitionCounter {

    /** Prazo para o cluster responder; acima disso a subida falha (não dá para provar a topologia). */
    static final int TIMEOUT_MS = 30_000;

    @Override
    public Map<String, Integer> partitionCounts(Map<String, Object> adminConfig, Set<String> topics)
            throws Exception {
        Map<String, Object> config = new HashMap<>(adminConfig);
        config.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        config.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        Map<String, Integer> counts = new HashMap<>();
        try (Admin admin = Admin.create(config)) {
            Map<String, KafkaFuture<TopicDescription>> futures = admin.describeTopics(topics).topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    counts.put(entry.getKey(), entry.getValue().get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                            .partitions().size());
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw e;
                    }
                    // tópico inexistente: fica ausente do mapa
                }
            }
        }
        return counts;
    }
}
