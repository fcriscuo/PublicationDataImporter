package org.batteryparkdev.io

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.io.FilenameUtils
import java.io.FileReader
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.Consumer
import java.util.function.Supplier

/*
Supplier that provides a Map of column headings (keys) and column indices
(values) for a specified delimited (i.e. comma, tab) file Path
 */
class CsvHeaderSupplier(aPath: Path) : Supplier<Map<String, Int>?> {
    private var headerMap: Map<String, Int>? = null
    init {
        try {
            val extension = FilenameUtils.getExtension(aPath.toString())
            FileReader(aPath.toString()).use {
                val parser = when (extension) {
                    "tsv" -> CSVParser.parse(
                        aPath.toFile(), Charset.defaultCharset(),
                        CSVFormat.TDF.withFirstRecordAsHeader()
                    )
                    else -> CSVParser.parse(
                        aPath.toFile(), Charset.defaultCharset(),
                        CSVFormat.RFC4180.withFirstRecordAsHeader()
                    )
                }
                headerMap = parser.headerMap
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    override fun get(): Map<String, Int>? {
        return headerMap
    }
}

fun main(args: Array<String>) {
    val aPath = Paths.get("./data/sample_CosmicMutantExport.tsv")
    val headerMap = CsvHeaderSupplier(aPath).get()
    headerMap!!.keys.forEach(Consumer { x: String? -> println(x) })
}