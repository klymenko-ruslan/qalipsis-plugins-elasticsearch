package io.qalipsis.plugins.graphite.events

import io.micronaut.context.annotation.Requires
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.plugins.graphite.events.codecs.GraphiteClientHandler
import io.qalipsis.plugins.graphite.events.codecs.GraphitePickleEncoder
import io.qalipsis.plugins.graphite.events.codecs.GraphitePlaintextEncoder
import io.qalipsis.plugins.graphite.events.model.GraphiteProtocolType
import io.qalipsis.plugins.graphite.events.model.EventsBuffer
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import mu.KotlinLogging
import java.lang.UnsupportedOperationException
import java.time.Duration

/**
 *
 */
@Singleton
@Requires(beans = [GraphiteEventsConfiguration::class])
class GraphiteEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    private val graphiteEventsConfiguration: GraphiteEventsConfiguration
) : AbstractBufferedEventsPublisher(
    graphiteEventsConfiguration.minLevel,
    Duration.ofSeconds(graphiteEventsConfiguration.batchFlushIntervalSeconds),
    graphiteEventsConfiguration.batchSize,
    coroutineScope
) {

    private val metricsBuffer = EventsBuffer()

    private lateinit var workerGroup: EventLoopGroup

    private lateinit var channelFuture: ChannelFuture

    override fun start() {
        buildClient()
        super.start()
    }

    private fun buildClient() {
        val host = graphiteEventsConfiguration.graphiteHost
        val port = graphiteEventsConfiguration.graphitePort
        val encoder = resolveProtocolEncoder()
        workerGroup = NioEventLoopGroup()

        try {
            val b = Bootstrap()
            b.group(workerGroup)
            b.channel(NioSocketChannel::class.java)
            b.handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(
                        encoder,
                        GraphiteClientHandler(metricsBuffer, graphiteEventsConfiguration, coroutineScope)
                    )
                }
            }).option(ChannelOption.SO_KEEPALIVE, true)
            channelFuture = b.connect(host, port).sync()
            log.info { "Graphite connection established. Host: " + graphiteEventsConfiguration.graphiteHost + ", port: " + graphiteEventsConfiguration.graphitePort + ", protocol: " + graphiteEventsConfiguration.protocol }
        } catch (e: Exception) {
            // reconnect
            log.warn { "Graphite connection was lost due to: " + e.message }
            buildClient()
        }
    }

    override fun stop() {
        super.stop()
        val shotdownFuture = workerGroup?.shutdownGracefully()
        while (!shotdownFuture.isDone) {
            Thread.sleep(200)
        }
        metricsBuffer.copyAndClear()
    }

    override suspend fun publish(values: List<Event>) {
        metricsBuffer.addAll(values)
    }

    private fun resolveProtocolEncoder() = when (graphiteEventsConfiguration.protocol) {
        GraphiteProtocolType.plaintext.name -> GraphitePlaintextEncoder()
        GraphiteProtocolType.pickle.name -> GraphitePickleEncoder()
        else -> throw UnsupportedOperationException("Unknown graphite protocol: " + graphiteEventsConfiguration.protocol)
    }

    companion object {
        @JvmStatic
        private val log = KotlinLogging.logger {}
    }

}
