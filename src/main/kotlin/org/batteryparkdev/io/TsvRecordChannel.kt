package org.batteryparkdev.io


import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import org.apache.commons.csv.CSVRecord
import java.nio.file.Paths

object TsvRecordChannel {

    @OptIn(ExperimentalCoroutinesApi::class)
     fun CoroutineScope.produceTSVRecords(filename: String) =
        produce<CSVRecord> {
            val path = Paths.get(filename)
            TsvRecordSequenceSupplier(path).get()
                .forEach {
                    send(it)
                    delay(20)
                }
        }
fun produceTSVRecordFlow (filename: String):Flow<CSVRecord> = flow{
    val path = Paths.get(filename)
    TsvRecordSequenceSupplier(path).get()
        .forEach {
            emit(it)
        }
}.flowOn(Dispatchers.IO)

    fun displayRecords(filename: String) = runBlocking {
        val records = produceTSVRecords(filename)
        for (record in records) {
            println(record)
        }
    }
}


fun main(args: Array<String>) {
    val filename = if (args.isNotEmpty()) args[0] else "./data/CosmicHGNC.tsv"
    TsvRecordChannel.displayRecords(filename)
}