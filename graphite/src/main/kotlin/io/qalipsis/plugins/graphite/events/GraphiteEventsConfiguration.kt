package io.qalipsis.plugins.graphite.events

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.qalipsis.api.events.EventLevel
import javax.validation.constraints.NotNull

/**
 * @author rklymenko
 */
@Requires(property = "metrics.graphite.enabled", value = "true")
@ConfigurationProperties("")
class GraphiteEventsConfiguration(
    @Value("\${metrics.graphite.host}") val graphiteHost: String,
    @Value("\${metrics.graphite.port}") val graphitePort: Int,
    @Value("\${metrics.graphite.protocol}") val protocol: String,
    @Value("\${metrics.graphite.batch-size}") val batchSize: Int,
    @Value("\${metrics.graphite.batch-flush-interval-seconds}") val batchFlushIntervalSeconds: Long
) {
    @field:NotNull
    var minLevel: EventLevel = EventLevel.INFO
}