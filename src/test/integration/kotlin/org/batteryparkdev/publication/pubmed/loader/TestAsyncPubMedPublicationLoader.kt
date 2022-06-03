package org.batteryparkdev.publication.pubmed.loader

class TestAsyncPubMedPublicationLoader {

    fun loadPubmedEntries(): String {
        val taskDuration = 900_000L
        val timerInterval = 60_000L
        val scanTimer = AsyncPubMedPublicationLoader.scheduledPlaceHolderNodeScan(timerInterval)
        try {
            Thread.sleep(taskDuration)
        } finally {
            scanTimer.cancel();
        }
        return "PubMed loaded"
    }
}

fun main() {
    TestAsyncPubMedPublicationLoader().loadPubmedEntries()
}