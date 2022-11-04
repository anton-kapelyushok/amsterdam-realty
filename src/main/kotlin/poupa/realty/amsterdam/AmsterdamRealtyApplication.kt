package poupa.realty.amsterdam

import com.pengrad.telegrambot.TelegramBot
import com.pengrad.telegrambot.request.SendMessage
import mu.KotlinLogging
import org.jsoup.Jsoup
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.repository.CrudRepository
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import kotlin.random.Random

private val log = KotlinLogging.logger {}

val chatId = -716389843

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
@EnableScheduling
class AmsterdamRealtyApplication(
    private val bot: TelegramBot,
    private val fundaListingsFetcher: FundaListingsFetcher,
) : ApplicationRunner {


    override fun run(args: ApplicationArguments?) {
//        val res = fundaListingsFetcher.fetch()
//        val req = SendMessage(-716389843, "Hello, world")
//        val res = bot.execute(GetUpdates())
//        val res = bot.execute(req)
    }
}

@Component
class CronChecker(
    private val parariusListingsFetcher: ParariusListingsFetcher,
    private val fundaListingsFetcher: FundaListingsFetcher,
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val telegramBot: TelegramBot,
) {
    @Scheduled(cron = "0 */5 * * * *")
    fun runUpdate() {
        val listings = listOf(
            parariusListingsFetcher,
            fundaListingsFetcher,
        ).flatMap {
            try {
                val res = it.fetch()
                if (res.isEmpty()) {
                    telegramBot.execute(SendMessage(chatId, "${it::class.java} returned empty result"))
                }
                res
            } catch (e: Throwable) {
                log.error(e) {}
                val sw = StringWriter()
                e.printStackTrace(PrintWriter(sw))
                val exceptionAsString = sw.toString()
                telegramBot.execute(SendMessage(chatId, exceptionAsString))
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
                            "link" to link, "source" to source, "name" to name, "price" to price, "address" to address
                        )
                    )
                }
            } catch (e: DuplicateKeyException) {
                exists = true
            }

            if (!exists) {
                log.info { "${listing.link} is new" }
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
            } else {
                log.info { "${listing.link} already exists" }
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
class ParariusListingsFetcher : ListingsFetcher {
    override fun fetch(): List<Listing> {
        val link = "https://www.pararius.nl/huurwoningen/amsterdam/1500-2000?query=${Random.nextInt(0, 1000000)}"
        val request = HttpRequest.newBuilder().uri(URI.create(link)).header(
            "accept",
            """text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"""
        ).header(
            "cookie",
            "latest_search_locations=%5B%22amsterdam%22%5D; path=/; domain=.www.pararius.nl; secure; samesite=lax"
        ).header(
            "user-agent",
            """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/${
                Random.nextInt(
                    400, 600
                )
            }.35"""
        ).build();
        val client = HttpClient.newBuilder().build()
        val text = client.send(request, BodyHandlers.ofString()).body()
//        val text = File("./pararius.html").readText()
        val f = Jsoup.parse(text)
        val names = f.select(".listing-search-item__title").map { it.text() }
        val addresses = f.select(".listing-search-item__sub-title").map { it.text() }
        val links = f.select(".listing-search-item__link--title").map { "https://www.pararius.nl" + it.attr("href") }
        val prices = f.select(".listing-search-item__price").map { it.text() }
        val source = "pararius"

        log.info { "Fetched ${names.size} items from pararius" }
        return names.indices.map { idx ->
            Listing(
                link = links[idx], name = names[idx], address = addresses[idx], price = prices[idx], source = source
            )
        }
    }
}


@Component
class FundaListingsFetcher : ListingsFetcher {
    override fun fetch(): List<Listing> {
        val link = "https://www.funda.nl/huur/amsterdam/1500-2000/sorteer-datum-af/"
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
        val names = f.select(".search-result__header-title").map { it.text() }
        val addresses = f.select(".search-result__header-subtitle").map { it.text() }
        val links =
            f.select(".search-result__header-title-col a:first-child").map { "https://www.funda.nl" + it.attr("href") }
        val prices = f.select(".search-result-price").map { it.text() }
        val source = "funda"

        log.info { "Fetched ${names.size} items from pararius" }
        return names.indices.map { idx ->
            Listing(
                link = links[idx], name = names[idx], address = addresses[idx], price = prices[idx], source = source
            )
        }
    }

}