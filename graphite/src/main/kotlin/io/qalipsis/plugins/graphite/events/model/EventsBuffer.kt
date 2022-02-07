package io.qalipsis.plugins.graphite.events.model

import io.qalipsis.api.events.Event

/**
 * @author rklymenko
 */
class EventsBuffer {

    private val dataBuffer: MutableList<Event> = mutableListOf()

    @Synchronized
    fun addAll(event: Collection<Event>) {
        dataBuffer.addAll(event)
    }

    @Synchronized
    fun copyAndClear(): List<Event> {
        val result = dataBuffer.toList()
        dataBuffer.clear()
        return result
    }

    @Synchronized
    fun poll(): List<Event>? {
        if (size() == 0) return null
        return listOf(dataBuffer.removeAt(0))
    }

    fun size() = dataBuffer.size
}