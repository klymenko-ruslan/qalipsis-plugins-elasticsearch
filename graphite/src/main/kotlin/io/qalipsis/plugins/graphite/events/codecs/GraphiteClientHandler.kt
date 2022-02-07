package io.qalipsis.plugins.graphite.events.codecs

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.qalipsis.plugins.graphite.events.GraphiteEventsConfiguration
import io.qalipsis.plugins.graphite.events.model.EventsBuffer
import io.qalipsis.plugins.graphite.events.model.GraphiteProtocolType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.lang.UnsupportedOperationException

/**
 * @author rklymenko
 */
class GraphiteClientHandler(
    val metricsBuffer: EventsBuffer,
    val graphiteEventsConfiguration: GraphiteEventsConfiguration,
    val coroutineScope: CoroutineScope
) : ChannelInboundHandlerAdapter() {

    override fun channelActive(ctx: ChannelHandlerContext) {
        coroutineScope.launch {
            writeFromBuffer(ctx)
        }
    }

    private fun writeFromBuffer(ctx: ChannelHandlerContext) {
        log.info { "Graphite reading from buffer started. Protocol: " + graphiteEventsConfiguration.protocol }
        if (graphiteEventsConfiguration.protocol == GraphiteProtocolType.pickle.name) {
            while (true) {
                writePickle(ctx)
            }
        } else if (graphiteEventsConfiguration.protocol == GraphiteProtocolType.plaintext.name) {
            while (true) {
                writePlaintext(ctx)
            }
        } else {
            throw UnsupportedOperationException("Unsupported graphite protocol.")
        }
    }

    private fun writePickle(ctx: ChannelHandlerContext) {
        if (metricsBuffer.size() > 0) {
            ctx.writeAndFlush(metricsBuffer.copyAndClear())
        }
        Thread.sleep(graphiteEventsConfiguration.batchFlushIntervalSeconds * 1000)
    }

    private fun writePlaintext(ctx: ChannelHandlerContext) {
        while (metricsBuffer.size() > 0) {
            ctx.writeAndFlush(metricsBuffer.poll())
        }
        Thread.sleep(graphiteEventsConfiguration.batchFlushIntervalSeconds * 1000)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.error { "Graphite error occured: " + cause }
        ctx.close()
    }

    companion object {
        @JvmStatic
        private val log = KotlinLogging.logger {}
    }
}