package org.batteryparkdev.publication.pubmed.service

import ai.wisecube.pubmed.PubmedArticle
import ai.wisecube.pubmed.PubmedParser
import arrow.core.Either
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.placeholder.model.NodeIdentifier
import org.batteryparkdev.placeholder.model.PlaceholderNode
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import java.io.StringReader
import java.net.URL
import java.nio.charset.Charset
import javax.xml.parsers.DocumentBuilderFactory

object PubMedRetrievalService {

    private val ncbiEmail = System.getenv("NCBI_EMAIL")
    private val ncbiApiKey = System.getenv("NCBI_API_KEY")
    private val dbFactory = DocumentBuilderFactory.newInstance()
    private val dBuilder = dbFactory.newDocumentBuilder()
    private val ncbiDelay: Long = 10L
    private const val pubMedTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
    private val citationTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_citedin" +
                "&id=PUBMEDID&&tool=my_tool&email=NCBIEMAIL&api_key=APIKEY"
    private val referenceTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_refs" +
                "&id=PUBMEDID&&tool=my_tool&email=NCBIEMAIL&api_key=APIKEY"
    private const val pubMedToken = "PUBMEDID"

    /*
  Return an Either<Exception, PubMedArticle to deal with NCBI
  service disruptions
   */
    fun retrievePubMedArticle(pubmedId: Int): Either<Exception, PubmedArticle> {
        Thread.sleep(ncbiDelay)  // Accommodate NCBI maximum request rate
        val url = pubMedTemplate
            .replace(pubMedToken, pubmedId.toString())
        return try {
            val text = URL(url).readText(Charset.defaultCharset())
            val parser = PubmedParser()
            val articleSet = parser.parse(text, ai.wisecube.pubmed.PubmedArticleSet::class.java)
            Either.Right(articleSet.pubmedArticleOrPubmedBookArticle[0] as PubmedArticle)
        } catch (e: Exception) {
            Either.Left(e)
        }
    }

    fun retrieveCitationIds(pubmedId: Int): Set<Int> {
        val url = citationTemplate.replace(pubMedToken, pubmedId.toString())
            .replace("NCBIEMAIL", ncbiEmail)
            .replace("APIKEY", ncbiApiKey)
        val citationSet = mutableSetOf<Int>()
        try {
            val text = URL(url).readText(Charset.defaultCharset())
            val xmlDoc = dBuilder.parse(InputSource(StringReader(text)));
            xmlDoc.documentElement.normalize()
            val citationList: NodeList = xmlDoc.getElementsByTagName("Link")
            for (i in 0 until citationList.length) {
                val citationNode = citationList.item(i)
                if (citationNode.nodeType == Node.ELEMENT_NODE) {
                    val elem = citationNode as Element
                    val id = elem.getElementsByTagName("Id").item(0).textContent
                    citationSet.add(id.toInt())
                }
            }
        } catch (e: Exception) {
            LogService.logError("++++  EXCEPTION getting citation set for $pubmedId")
            LogService.logException(e)
        }
        return citationSet.toSet()
    }

    /*
    Function to generate a Set of PlaceholderNode objects representing
    the articles referenced by the specified PubMed article
    These PlaceholderNode objects are labeled "Reference"
     */
    fun generateReferencePlaceholderNodes(pubmedId: Int): Set<PlaceholderNode> {
        val placeholderSet = mutableSetOf<PlaceholderNode>()
        retrieveReferenceIds(pubmedId).forEach { ref ->
            run {
                val parentNode = NodeIdentifier("Publication", "pub_id", pubmedId.toString(),
                "PubMed")
                val childNode = NodeIdentifier(
                    "Publication", "pub_id", ref.toString(),
                    "Reference"
                )
                placeholderSet.add(PlaceholderNode(parentNode, childNode, "HAS_REFERENCE",
                    "title"))
            }
        }
        return placeholderSet.toSet()
    }

    /*
    Function to retrieve the PubMed Ids of the articles referenced by
    the specified PubMed Id
     */
    private fun retrieveReferenceIds(pubmedId: Int): Set<Int> {
        val url = referenceTemplate.replace(pubMedToken, pubmedId.toString())
            .replace("NCBIEMAIL", ncbiEmail)
            .replace("APIKEY", ncbiApiKey)
        val referenceSet = mutableSetOf<Int>()
        try {
            val text = URL(url).readText(Charset.defaultCharset())
            val xmlDoc = dBuilder.parse(InputSource(StringReader(text)));
            xmlDoc.documentElement.normalize()
            val referenceList: NodeList = xmlDoc.getElementsByTagName("Link")
            for (i in 0 until referenceList.length) {
                val referenceNode = referenceList.item(i)
                if (referenceNode.nodeType == Node.ELEMENT_NODE) {
                    val elem = referenceNode as Element
                    val id = elem.getElementsByTagName("Id").item(0).textContent
                    referenceSet.add(id.toInt())
                }
            }
        } catch (e: Exception) {
            LogService.logError("++++  EXCEPTION getting reference set for $pubmedId")
            LogService.logException(e)
        }
        LogService.logFine("+++Reference set size = ${referenceSet.size}")
        return referenceSet.toSet()
    }
}

fun main() {
    // test PubMedArticle retrieval
    when (val retEither = PubMedRetrievalService.retrievePubMedArticle(26050619)) {
        is Either.Right -> {
            val article = retEither.value
            println("Title: ${article.medlineCitation.article.articleTitle.getvalue()}")
            PubMedRetrievalService.retrieveCitationIds(26050619).stream()
                .forEach { cit -> println(cit) }
            PubMedRetrievalService.generateReferencePlaceholderNodes(26050619).stream()
                .forEach { ref -> println("Placeholder $ref") }
        }
        is Either.Left -> {
            LogService.logException( retEither.value)
        }
    }
}