import analyzer.NamedEntityRecognitionHandler;
import analyzer.SentimentAnalysisHandler;

public class Worker {
    public static void main(String[] args) {
        SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
        NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

        // Get message from SQS queue

        // Perform sentiment analysis

        // Perform named entity recognition

        // Remove message from SQS queue

        // IMPORTANT: If a worker stops working unexpectedly before finishing its work on a message, then some other worker should be able to handle that message.


    }

}
