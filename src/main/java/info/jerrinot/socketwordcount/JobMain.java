package info.jerrinot.socketwordcount;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.Sources.socket;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static info.jerrinot.socketwordcount.IngestionTimeUtils.heartbeater;

public final class JobMain {
    private static final int AUTO_WATERMARK_INTERVAL_MILLIS = 200;
    private static final FunctionEx<StreamStage<String>, StreamStage<String>> TOKENIZER = s -> s.flatMap(e -> traverseArray(e.split("\\s")));

    public static void main(String[] args) {
        JetInstance jetInstance = Jet.bootstrappedInstance();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(socket("localhost", 9000))
                .withIngestionTimestamps()
                .apply(heartbeater(AUTO_WATERMARK_INTERVAL_MILLIS))
                .apply(TOKENIZER)
                .groupingKey(wholeItem())
                .window(tumbling(5_000))
                .aggregate(counting())
                .writeTo(logger());

        jetInstance.newJob(pipeline).join();
    }

}
