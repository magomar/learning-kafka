package org.learning.kafka;

import org.learning.kafka.elasticsearch.ElasticSearchKafkaConsumer;
import org.learning.kafka.twitter.TwitterKafkaProducer;
import picocli.CommandLine;

@CommandLine.Command(subcommands = {
        ProduceCommand.class,
        ConsumeCommand.class
})
public class TwittyCLI {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TwittyCLI()).execute(args);
        System.exit(exitCode);
    }
}

@CommandLine.Command(name = "produce")
class ProduceCommand implements Runnable {

    @CommandLine.Parameters(arity = "1..*", paramLabel = "<keywords>", description = "Terms to search for in tweets")
    private String[] keywords;

    @Override
    public void run() {
        TwitterKafkaProducer.startProducer(keywords);
    }
}

@CommandLine.Command(name = "consume")
class ConsumeCommand implements Runnable {

    @Override
    public void run() {
        ElasticSearchKafkaConsumer.startConsumers();
    }
}