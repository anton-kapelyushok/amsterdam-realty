package poupa.realty.amsterdam

import com.pengrad.telegrambot.TelegramBot
import com.pengrad.telegrambot.request.SendMessage
import mu.KotlinLogging
import org.jsoup.Jsoup
import org.openqa.selenium.By
import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.firefox.FirefoxOptions
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.reflect.KClass


private val log = KotlinLogging.logger {}

val chatId = -716389843
val meId = 128947731

val token = System.getProperty("TG_TOKEN") ?: runCatching { File("./.token").readText() }.getOrNull()
?: error("could not find token")

@Configuration
class TelegramBotConfiguration {
    @Bean
    fun telegramBot(): TelegramBot {
        return TelegramBot(token)
    }
}

@SpringBootApplication
class AmsterdamRealtyApplication(
    private val bot: TelegramBot,
    private val listingsChecker: ListingsChecker,
    private val syncTelegramListingHandler: SyncTelegramListingHandler,
    private val asyncTelegramListingHandler: AsyncTelegramListingHandler,
    private val openInBrowserListingHandler: OpenInBrowserListingHandler,
    private val fundaListingsFetcher: FundaListingsFetcher,
    private val parariusListingsFetcher: ParariusListingsFetcher,
) : ApplicationRunner {

    override fun run(args: ApplicationArguments?) {
        System.setProperty(
            "webdriver.gecko.driver",
            "/Users/Anton.Kapeliushok/Projects/amsterdam-realty/lib/geckodriver"
        )

        val allFetchers = listOf(fundaListingsFetcher, parariusListingsFetcher)

        if (args?.containsOption("watch") == true) {
            try {
                val res1 = bot.execute(SendSilentMessage(meId, "I'm alive!"))
                while (true) {
                    try {
                        listingsChecker.runUpdate(
                            allFetchers,
                            listOf(syncTelegramListingHandler)
                        )
                    } catch (e: Exception) {
                        log.warn(e) { "Failed to runUpdate, rescheduling " }
                    }
                    Thread.sleep(Duration.ofMinutes(10).toMillis())
                }
            } finally {
                bot.execute(SendSilentMessage(meId, "I'm dying :("))
            }
        } else {
            listingsChecker.runUpdate(allFetchers, listOf(asyncTelegramListingHandler, openInBrowserListingHandler))
            asyncTelegramListingHandler.awaitQueueIsEmpty()
        }
    }
}

fun SendSilentMessage(chatId: Any, text: String) = SendMessage(chatId, text).apply {
    parameters["disable_notification"] = true
}

@Component
class ListingsChecker(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val telegramBot: TelegramBot,
) {

    private val unhealthyFetchers = mutableSetOf<KClass<*>>()

    fun runUpdate(
        listingFetchers: List<ListingsFetcher>,
        listingHandlers: List<ListingHandler>,
    ) {
        Executors.newFixedThreadPool(listingHandlers.size).with { handlersExecutor ->
            val listings = listingFetchers.flatMap {
                try {
                    val res = it.fetch()
                    if (res.isEmpty()) {
                        if (it::class !in unhealthyFetchers) {
                            telegramBot.execute(SendSilentMessage(meId, "${it::class.java} returned empty result"))
                        } else {
                            unhealthyFetchers += it::class
                        }
                    } else {
                        if (it::class in unhealthyFetchers) {
                            unhealthyFetchers -= it::class
                            telegramBot.execute(SendSilentMessage(meId, "${it::class.java} restored!"))
                        }
                    }
                    res
                } catch (e: Throwable) {
                    log.error(e) {}
                    unhealthyFetchers += it::class
                    val sw = StringWriter()
                    sw.write(it::class.toString() + ":\n")
                    e.printStackTrace(PrintWriter(sw))
                    val exceptionAsString = sw.toString()
                    telegramBot.execute(SendMessage(meId, exceptionAsString))
                    listOf()
                }
            }
            for (listing in listings) {
                var exists = false
                try {
                    with(listing) {
                        jdbcTemplate.update(
                            // language=SQL
                            """
                    INSERT INTO processed_listings (link, source, name, price, address)
                    VALUES (:link, :source, :name, :price, :address)
                """.trimIndent(), mapOf(
                                "link" to link,
                                "source" to source,
                                "name" to name,
                                "price" to price,
                                "address" to address
                            )
                        )
                    }
                } catch (e: DuplicateKeyException) {
                    exists = true
                }

                if (!exists) {
                    log.info { "${listing.link} is new" }

                    listingHandlers.map { listingHandler ->
                        listingHandler to handlersExecutor.submit {
                            try {
                                listingHandler.handleListing(listing)
                            } catch (e: Throwable) {
                                log.warn(e) { "${listingHandler::class.simpleName} handleListing failed" }
                            }
                        }
                    }.forEach { (listingHandler, future) ->
                        try {
                            future.get(5, TimeUnit.SECONDS)
                        } catch (e: Throwable) {
                            log.warn(e) { "${listingHandler::class.simpleName} future await failed" }
                        }
                    }
                } else {
                    log.info { "${listing.link} already exists" }
                }
            }
        }
    }
}


fun main(args: Array<String>) {
    runApplication<AmsterdamRealtyApplication>(*args)
}

interface ListingsFetcher {
    fun fetch(): List<Listing>
}

data class Listing(
    val source: String,
    val link: String,
    val name: String,
    val price: String,
    val address: String,
)

@Component
class ParariusListingsFetcher(
    private val driverFactory: DriverFactory,
) : ListingsFetcher {
    override fun fetch(): List<Listing> {

        val webDriver = driverFactory.webDriver()
        val text = try {
            webDriver.navigate().to("https://www.pararius.nl/huurwoningen/amsterdam/1200-2350")

            val start = System.currentTimeMillis()
            while (true) {
                try {
                    val now = System.currentTimeMillis()
                    if (Duration.ofMillis(now - start) > Duration.ofSeconds(10)) {
                        log.info { "Pararius timeout :(" }
                        return emptyList()
                    }
                    try {
                        webDriver.findElement(By.className("search-list-sorting"))
                        break
                    } catch (e: org.openqa.selenium.NoSuchElementException) {
                        Thread.sleep(300)
                        continue
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return listOf()
                } catch (e: Throwable) {
                    // other exception
                    throw e;
                }
            }

            webDriver.pageSource
        } finally {
            for (i in 1..10) {
                try {
                    webDriver.quit()
                    break
                } catch (e: Throwable) {
                    if (i == 10) {
                        log.error(e) { }
                    }
                }
            }
        }

        val f = Jsoup.parse(text)
        val names = f.select(".listing-search-item__title").map { it.text() }
        val addresses = f.select(".listing-search-item__title")
            .map { it.siblingElements().find { it.className() == "listing-search-item__sub-title'" } }
            .map { it?.text() ?: "????" }
        val links = f.select(".listing-search-item__link--title").map { "https://www.pararius.nl" + it.attr("href") }
        val prices = f.select(".listing-search-item__price").map { it.text() }
        val source = "pararius"

        log.info { "Fetched ${names.size} items from pararius" }
        return names.indices.map { idx ->
            Listing(
                link = links[idx],
                name = names[idx],
                address = addresses[idx],
                price = prices[idx],
                source = source
            )
        }
    }
}


@Component
class DriverFactory {

    // https://www.zenrows.com/blog/undetected-chromedriver
    fun webDriver(): FirefoxDriver {
//        FirefoxDriverManager.getInstance().setup()
        val options = FirefoxOptions()
//        options.binary = FirefoxBinary(File("./lib/geckodriver"))
//        options.addArguments("--headless")
        options.addArguments("--window-size=1280,939")
        options.addArguments("""--user-agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0"""")
        return FirefoxDriver(options)
    }
}

@Component
class FundaListingsFetcher : ListingsFetcher {
    override fun fetch(): List<Listing> {
        val link =
            "https://www.funda.nl/zoeken/huur?selected_area=%5B%22amsterdam%22%5D&price=%221750-2350%22&sort=%22date_down%22&energy_label=%5B%22A%22,%22A%2B%22,%22A%2B%2B%22,%22A%2B%2B%2B%22,%22A%2B%2B%2B%2B%22,%22A%2B%2B%2B%2B%2B%22%5D"
        val request = HttpRequest.newBuilder().uri(URI.create(link)).header(
            "accept",
            """text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"""
        ).header(
            "user-agent",
            """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/${
                Random.nextInt(
                    400, 600
                )
            }.35"""
        ).build()
        val client = HttpClient.newBuilder().build()
        val text = client.send(request, BodyHandlers.ofString()).body()
//        val text = File("./funda.html").readText()
        val f = Jsoup.parse(text)
        val names = f.select("[data-test-id=street-name-house-number]").map { it.text() }
        val addresses = f.select("[data-test-id=postal-code-city]").map { it.text() }
        val links =
            f.select("[data-test-id=street-name-house-number]")
                .map { it.parent()!! }
                .map { it.attr("href") }
        val prices = f.select("[data-test-id=price-rent]").map { it.text() }
        val source = "funda"

        log.info { "Fetched ${names.size} items from funda" }
        return names.indices.map { idx ->
            Listing(
                link = links[idx], name = names[idx], address = addresses[idx], price = prices[idx], source = source
            )
        }
    }
}


interface ListingHandler {
    fun handleListing(listing: Listing)
}

@Component
class AsyncTelegramListingHandler(
    private val telegramBot: TelegramBot,
) : ListingHandler {

    private val q = LinkedBlockingQueue<Listing>()

    override fun handleListing(listing: Listing) {
        q.add(listing)
    }

    @PostConstruct
    fun handleQueue() {
        thread(start = true, isDaemon = true) {
            while (true) {
                val listing = q.take()
                try {
                    telegramBot.execute(
                        SendMessage(
                            chatId, """
                    ${listing.name}
                    ${listing.address}
                    ${listing.price}
                    ${listing.link}
                """.trimIndent()
                        )
                    )
                } catch (e: Exception) {
                    log.warn(e) { "Failed to send listing to tg ${listing.link}: ${e.message}" }
                }
                Thread.sleep(500)
            }
        }
    }

    fun awaitQueueIsEmpty() {
        log.info { "awaiting tg queue" }
        while (q.isNotEmpty()) {
            Thread.sleep(100)
        }
        log.info { "tg queue is empty" }
    }
}

@Component
class SyncTelegramListingHandler(
    private val telegramBot: TelegramBot,
) : ListingHandler {
    override fun handleListing(listing: Listing) {
        telegramBot.execute(
            SendMessage(
                chatId, """
                    ${listing.name}
                    ${listing.address}
                    ${listing.price}
                    ${listing.link}
                """.trimIndent()
            )
        )
        Thread.sleep(1000)
    }
}

@Component
class OpenInBrowserListingHandler : ListingHandler {
    override fun handleListing(listing: Listing) {
        ProcessBuilder("open", listing.link).start()
        Thread.sleep(200)
    }
}

private fun ExecutorService.with(fn: (es: ExecutorService) -> Unit) {
    try {
        fn(this)
    } finally {
        this.shutdown()
    }
}