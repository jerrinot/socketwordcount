package info.jerrinot.socketwordcount;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.impl.pipeline.AbstractStage;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.function.PredicateEx.alwaysFalse;

public final class IngestionTimeUtils {
    private IngestionTimeUtils() {

    }

    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> heartbeater(long intervalMillis) {
        return s -> {
            adjustIdleTimeout(s, intervalMillis / 2);
            Pipeline pipeline = s.getPipeline();
            StreamStage<?> heartbeatStage = pipeline.readFrom(newHeartbeatingSource(intervalMillis))
                    .withIngestionTimestamps()
                    .filter(alwaysFalse());

            return s.merge((StreamStage<? extends T>) heartbeatStage);
        };
    }

    private static StreamSource<?> newHeartbeatingSource(long intervalMillis) {
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        return SourceBuilder.stream("heartbeater", e -> new MutableLong())
                .fillBufferFn((ts, out) -> {
                    long now = System.nanoTime();
                    long last = ts.value;
                    if (now > last + intervalNanos) {
                        out.add(0);
                        ts.value = now;
                    }
                }).distributed(1)
                .build();
    }

    private static <T> void adjustIdleTimeout(StreamStage<T> s, long timeoutMillis) {
        Transform transform = AbstractStage.transformOf(s);
        if (!(transform instanceof StreamSourceTransform)) {
            throw new IllegalStateException("Heartbeater has to be attached immediately after a source!");
        }
        StreamSourceTransform<T> streamSourceTransform = (StreamSourceTransform<T>) transform;
        streamSourceTransform.setPartitionIdleTimeout(timeoutMillis);
        EventTimePolicy<? super T> oldPolicy = streamSourceTransform.getEventTimePolicy();
        EventTimePolicy<? super T> newPolicy = newPolicyWithDifferentTimeout(oldPolicy, timeoutMillis);
        streamSourceTransform.setEventTimePolicy(newPolicy);
    }

    private static <T> EventTimePolicy<? super T> newPolicyWithDifferentTimeout(EventTimePolicy<? super T> oldPolicy, long idleTimeoutMillis) {
        if (oldPolicy == null) {
            throw new IllegalStateException("Heartbeater can only be attached to a source with timestamps assigned");
        }
        EventTimePolicy<? super T> newPolicy = EventTimePolicy.eventTimePolicy(oldPolicy.timestampFn(),
                oldPolicy.wrapFn(),
                oldPolicy.newWmPolicyFn(),
                oldPolicy.watermarkThrottlingFrameSize(),
                oldPolicy.watermarkThrottlingFrameOffset(),
                idleTimeoutMillis);
        return newPolicy;
    }
}