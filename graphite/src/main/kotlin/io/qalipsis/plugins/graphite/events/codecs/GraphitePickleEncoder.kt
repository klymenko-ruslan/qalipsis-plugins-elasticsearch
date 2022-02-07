package io.qalipsis.plugins.graphite.events.codecs

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import io.qalipsis.api.events.Event
import mu.KotlinLogging
import java.nio.ByteBuffer

/**
 * @author rklymenko
 */
internal class GraphitePickleEncoder : MessageToByteEncoder<List<Event>>() {

    private val MARK = '('
    private val STOP = '.'
    private val LONG = 'L'
    private val STRING = 'S'
    private val APPEND = 'a'
    private val LIST = 'l'
    private val TUPLE = 't'
    private val QUOTE = '\''
    private val LF = '\n'
    private val SEMICOLON = ";"
    private val EQ = "="

    override fun encode(
        ctx: ChannelHandlerContext,
        events: List<Event>, out: ByteBuf
    ) {
        log.info { "Encoding events: " + events }
        val message = generateEvent(events)
        out.writeBytes(message)
        ctx.writeAndFlush(out)
        log.info { "Events flushed: " + events }
        out.retain()
    }

    private fun generateEvent(events: List<Event>): ByteArray {
        val payload = generatePayload(events)
        val payloadBytes = payload.toByteArray()
        val header = ByteBuffer.allocate(4).putInt(payloadBytes.size).array()
        return header + payloadBytes
    }

    private fun generatePayload(events: List<Event>): String {
        val payload = StringBuilder()
        payload.append(MARK)
        payload.append(LIST)

        for (tuple in events) {
            payload.append(MARK)
            payload.append(STRING)
            payload.append(QUOTE)
            payload.append(tuple.name)
            for (tag in tuple.tags) {
                payload.append(SEMICOLON)
                payload.append(tag.key)
                payload.append(EQ)
                payload.append(tag.value)
            }
            payload.append(QUOTE)
            payload.append(LF)
            payload.append(MARK)
            payload.append(LONG)
            payload.append(tuple.timestamp.toEpochMilli() / 1000)
            payload.append(LONG)
            payload.append(LF)
            payload.append(STRING)
            payload.append(QUOTE)
            payload.append(tuple.value)
            payload.append(QUOTE)
            payload.append(LF)
            payload.append(TUPLE)
            payload.append(TUPLE)
            payload.append(APPEND)
        }

        payload.append(STOP)
        return payload.toString()
    }

    companion object {
        @JvmStatic
        private val log = KotlinLogging.logger {}
    }
}