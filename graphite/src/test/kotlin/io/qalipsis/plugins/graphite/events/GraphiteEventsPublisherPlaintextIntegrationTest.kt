package io.qalipsis.plugins.graphite.events

import io.qalipsis.plugins.graphite.events.model.GraphiteProtocolType

/**
 * @author rklymenko
 */
internal class GraphiteEventsPublisherPlaintextIntegrationTest: AbstractGraphiteEventsPublisherIntegrationTest(GraphiteProtocolType.plaintext)