package org.batteryparkdev.io

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.batteryparkdev.logging.service.LogService
import java.io.FileReader
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Stream
import kotlin.streams.asSequence

class TsvRecordSequenceSupplier : Supplier<Sequence<CSVRecord>> {

    private var recordSequence: Sequence<CSVRecord> = Stream.empty<CSVRecord?>().asSequence()

    constructor (aPath: Path) {
        try {
            FileReader(aPath.toString()).use {
                val parser = CSVParser.parse(
                    aPath.toFile(), Charset.defaultCharset(),
                    CSVFormat.TDF.withFirstRecordAsHeader().withQuote(null).withIgnoreEmptyLines()
                )
                recordSequence = parser.records.asSequence()
            }
        } catch (e: IOException) {
            LogService.logException(e)
        }
    }

    /*
   Constructs a TsvRecordSequenceSupplier with a specified Array of column
   headings. This is to support parsing of large TSV files that need to be split into
   multiple files.
   These files should not contain a header and the first file from the Linux split
   command may need to be manually edited to remove the original header.
    */
    constructor(
        aPath: Path,
        vararg columnHeadings: String?
    ) {
        try {
            FileReader(aPath.toString()).use {
                val parser = CSVParser.parse(
                    aPath.toFile(), Charset.defaultCharset(),
                    CSVFormat.TDF.withHeader(*columnHeadings).withQuote(null).withIgnoreEmptyLines()
                )
                recordSequence = parser.records.asSequence()
            }
        } catch (e: IOException) {
            println(e.message)
            e.printStackTrace()
        }
    }

    override fun get(): Sequence<CSVRecord> {
        return recordSequence
    }
}

fun main(args: Array<String>) {
    val filePathName = if (args.isNotEmpty()) args[0] else "./data/sample_CosmicMutantExportCensus.tsv"
    val aPath = Paths.get(filePathName)
   println("Processing delimited file: $filePathName")
    val headerMap = CsvHeaderSupplier(aPath).get()
    TsvRecordSequenceSupplier(aPath).get()
        .take(100)
        .forEach { record: CSVRecord ->
            headerMap?.keys?.forEach(Consumer { key: String ->
                println("*** column: $key  value= ${record[key]}")
            })
        }
}