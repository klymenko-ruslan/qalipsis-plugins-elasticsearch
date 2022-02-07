package io.qalipsis.plugins.graphite.events

import io.micronaut.http.HttpStatus
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventTag
import io.qalipsis.plugins.graphite.events.model.GraphiteProtocolType
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import org.junit.Assert
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * @author rklymenko
 */
@Testcontainers
@Timeout(3, unit = TimeUnit.MINUTES)
abstract class AbstractGraphiteEventsPublisherIntegrationTest(val protocol: GraphiteProtocolType) {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var graphiteEventsConfiguration: GraphiteEventsConfiguration

    private val container = CONTAINER

    private var containerHttpPort = -1

    private val httpClient = createSimpleHttpClient()

    private val coroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private lateinit var graphiteEventsPublisher: GraphiteEventsPublisher

    private var protocolPort = -1

    companion object {

        const val GRAPHITE_IMAGE_NAME = "graphiteapp/graphite-statsd"
        const val HTTP_PORT = 80
        const val GRAPHITE_PLAINTEXT_PORT = 2003
        const val GRAPHITE_PICKLE_PORT = 2004
        const val LOCALHOST_HOST = "0.0.0.0"

        @Container
        @JvmStatic
        private val CONTAINER = GenericContainer<Nothing>(
            DockerImageName.parse(GRAPHITE_IMAGE_NAME)
        ).apply {
            setWaitStrategy(HostPortWaitStrategy())
            withExposedPorts(HTTP_PORT, GRAPHITE_PLAINTEXT_PORT, GRAPHITE_PICKLE_PORT)
            withAccessToHost(true)
            withStartupTimeout(Duration.ofSeconds(60))
            withCreateContainerCmdModifier { it.hostConfig!!.withMemory((512 * 1e20).toLong()).withCpuCount(2) }
        }
    }

    @BeforeAll
    fun setUp() {
        containerHttpPort = container.getMappedPort(HTTP_PORT)
        val request = generateHttpGet("http://$LOCALHOST_HOST:${containerHttpPort}/render")
        while(httpClient.send(request, HttpResponse.BodyHandlers.ofString()).statusCode() != HttpStatus.OK.code) {
            Thread.sleep(1_000)
        }

        protocolPort =
            if(protocol == GraphiteProtocolType.pickle) container.getMappedPort(GRAPHITE_PICKLE_PORT)
            else container.getMappedPort(GRAPHITE_PLAINTEXT_PORT)

        graphiteEventsConfiguration = GraphiteEventsConfiguration(
            "$LOCALHOST_HOST",
            protocolPort,
            protocol.name,
            1,
            1)

        graphiteEventsPublisher = GraphiteEventsPublisher(
            coroutineScope,
            graphiteEventsConfiguration
        )
        graphiteEventsPublisher.start()
    }

    @AfterAll
    fun close() {
        CONTAINER.stop()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `should save single event into graphite`() = testDispatcherProvider.runTest {
        //given
        val key = "my.test.path$protocol"
        val event = Event(key, EventLevel.INFO, emptyList(), 123)

        val request = generateHttpGet("http://${graphiteEventsConfiguration.graphiteHost}:${containerHttpPort}/render?target=$key&format=json")

        //when
        async {
            graphiteEventsPublisher.publish(event)
        }

        //then
        while(!httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body().contains(key)) {
            Thread.sleep(200)
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `should save multiple events one by one into graphite`() = testDispatcherProvider.runTest {
        //given
        val keys = (1 .. 5).map { "my.tests-separate$protocol.path$it" }
        val events = keys.map { Event(it, EventLevel.INFO, emptyList(), 123) }

        //when
        async {
            events.forEach { graphiteEventsPublisher.publish(it) }
        }

        //then
        for(key in keys) {
            val request = generateHttpGet("http://${graphiteEventsConfiguration.graphiteHost}:${containerHttpPort}/render?target=$key&format=json")

            while(!httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body().contains(key)) {
                Thread.sleep(200)
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `should save multiple events alltogether into graphite`() = testDispatcherProvider.runTest {
        //given
        val keys = (1 .. 5).map { "my.tests-alltogether$protocol.path$it" }
        val events = keys.map { Event(it, EventLevel.INFO, emptyList(), 123) }

        //when
        async {
            graphiteEventsPublisher.publish(events)
        }

        //then
        for(key in keys) {
            val request = generateHttpGet("http://${graphiteEventsConfiguration.graphiteHost}:${containerHttpPort}/render?target=$key&format=json")

            while(!httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body().contains(key)) {
                Thread.sleep(200)
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `should save single event into graphite with tags`() = testDispatcherProvider.runTest {
        //given
        val key = "my.test.tagged$protocol"
        val event = Event(key, EventLevel.INFO, listOf(EventTag("a", "1"), EventTag("b", "2")), 123.123)

        val url = StringBuilder()
        url.append("http://${graphiteEventsConfiguration.graphiteHost}:${containerHttpPort}/render?target=$key")
        for(tag in event.tags) {
            url.append(";")
            url.append(tag.key)
            url.append("=")
            url.append(tag.value)
        }
        url.append("&format=json")
        val request = generateHttpGet(url.toString())

        //when
        async {
            graphiteEventsPublisher.publish(event)
        }

        //then
        while(!httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body().contains(key)) {
            Thread.sleep(200)
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `should save nothing due to insufficient event level`() = testDispatcherProvider.runTest {
        //given
        val key = "fakekey$protocol"
        val event = Event(key, EventLevel.TRACE, emptyList(), 123.123)

        val request = generateHttpGet("http://${graphiteEventsConfiguration.graphiteHost}:${containerHttpPort}/render?target=$key&format=json")

        //when
        graphiteEventsPublisher.publish(event)
        Thread.sleep(2_000)

        //then
        Assert.assertFalse(httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body().contains(key))
    }

    protected fun generateHttpGet(uri: String) =
        HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(uri))
            .build()

    protected fun createSimpleHttpClient() =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build()
}