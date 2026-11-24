package kafka_consumer;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/delay")
public class KafkaMessageListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    private static final String INSERT_SQL =
            "INSERT INTO kafka_messages (\"msgUuid\", \"head\", \"timeRq\") VALUES (?, ?, ?) ON CONFLICT (\"msgUuid\") DO NOTHING";

    private static final Pattern UUID_PATTERN = Pattern.compile("\"msg_uuid\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern HEAD_PATTERN = Pattern.compile("\"head\"\\s*:\\s*(true|false)");

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong delayMs;

    public KafkaMessageListener(
            JdbcTemplate jdbcTemplate,
            @Value("${app.db-write-delay-ms:1000}") long initialDelayMs) {
        this.jdbcTemplate = jdbcTemplate;
        this.delayMs = new AtomicLong(initialDelayMs);
    }

    @GetMapping
    public Map<String, Long> getDelay() {
        return Map.of("delayMs", delayMs.get());
    }

    @RequestMapping(value = "/{ms}", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> setDelay(@PathVariable("ms") long ms) {
        if (ms < 0) {
            return Map.of("error", "Задержка не может быть отрицательной");
        }
        long oldDelay = delayMs.getAndSet(ms);
        log.warn("[Config Updated] Задержка записи в БД изменена: {} мс -> {} мс", oldDelay, ms);
        return Map.of(
                "status", "updated",
                "previousDelayMs", oldDelay,
                "currentDelayMs", ms
        );
    }

    @KafkaListener(topics = "test-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String rawMessage) {
        try {
            long readUnixTime = Instant.now().getEpochSecond();
            log.info("[Read from Kafka] {}", rawMessage);

            Matcher uuidMatcher = UUID_PATTERN.matcher(rawMessage);
            Matcher headMatcher = HEAD_PATTERN.matcher(rawMessage);

            if (!uuidMatcher.find() || !headMatcher.find()) {
                log.error("[Invalid JSON format] payload={}", rawMessage);
                return;
            }

            String msgUuid = uuidMatcher.group(1);
            boolean head = Boolean.parseBoolean(headMatcher.group(1));

            //Искусственная задержка перед записью в БД
            long currentDelay = delayMs.get();
            if (currentDelay > 0) {
                Thread.sleep(currentDelay);
            }

            jdbcTemplate.update(INSERT_SQL, msgUuid, head, readUnixTime);

            log.info("[Write to DB] { \"msgUuid\": \"{}\", \"head\": {}, \"timeRq\": \"{}\" }",
                    msgUuid, head, readUnixTime);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[Consumer Interrupted] Поток вычитки прерван");
        } catch (Exception e) {
            log.error("[Error processing message] payload={}, error={}", rawMessage, e.getMessage());
        }
    }
}