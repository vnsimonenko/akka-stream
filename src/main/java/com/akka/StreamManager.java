package com.akka;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.io.Framing;
import akka.stream.io.SynchronousFileSink;
import akka.stream.io.SynchronousFileSource;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamManager {
    static private org.slf4j.Logger log = LoggerFactory.getLogger(StreamManager.class);
    static final private Pattern dataPattern = Pattern.compile("^([\\s\\w\\d]+);([\\d\\.]+).*");

    interface TypeAndOperationResolver<T> {
        T valueOf(String s);

        T compute(T left, T right);

        T empty();
    }

    public static void collectToFile(String sysName, String inPath, String outPath, TypeAndOperationResolver typeResolver) throws IOException {
        final ActorSystem system = ActorSystem.create(sysName);
        final ActorMaterializer materializer = createMaterializer(system);

        Sink<ByteString, Future<Long>> fileSink = SynchronousFileSink.create(new File(outPath));
        Source<Future<Pair<String, Object>>, Future<Long>> futureSource = createSource(inPath, typeResolver, materializer);

        futureSource.runFold(0, (counter, future) -> counter + 1, materializer)
                .onComplete(new OnComplete<Integer>() {
                    @Override
                    public void onComplete(Throwable throwable, Integer uniqueCounter) throws Throwable {
                        futureSource
                                .buffer(uniqueCounter, OverflowStrategy.fail())
                                .mapAsync(1, i -> i)
                                .map(pair -> ByteString.fromString(String.format("%s;%s\n", pair.first(), pair.second())))
                                .runWith(fileSink, materializer).onComplete(new OnComplete<Long>() {
                            @Override
                            public void onComplete(Throwable throwable, Long longFuture) throws Throwable {
                                system.terminate();
                            }
                        }, system.dispatcher());
                    }
                }, system.dispatcher());
    }

    public static void collectToFileAndConsole(String sysName, String inPath, String outPath, TypeAndOperationResolver typeResolver,
                                               int maximumDistinctRecordKeys) throws IOException {
        final ActorSystem system = ActorSystem.create(sysName);
        final ActorMaterializer materializer = createMaterializer(system);

        Sink<ByteString, Future<Long>> fileSink = SynchronousFileSink.create(new File(outPath));
        Sink<ByteString, Future<BoxedUnit>> consoleSink = Sink.foreach(bs -> System.out.print("[id;val] " + bs.utf8String()));

        Source<Future<Pair<String, Object>>, Future<Long>> futureSource = createSource(inPath, typeResolver, materializer);
        final Source<ByteString, Future<Long>> sum = futureSource
                .buffer(maximumDistinctRecordKeys, OverflowStrategy.fail()) //an explicit failure instead of a silent deadlock
                .mapAsync(4, i -> i)
                .map(pair -> ByteString.fromString(String.format("%s;%s\n", pair.first(), pair.second())));

        final Graph<ClosedShape, Future<Long>> graph = FlowGraph.create(fileSink, (b, out) -> {
            final UniformFanOutShape<ByteString, ByteString> bcast = b.add(Broadcast.<ByteString>create(2));
            b.from(b.add(sum)).viaFanOut(bcast).to(out).from(bcast).to(b.add(consoleSink));
            return ClosedShape.getInstance();
        });

        final Future<Long> future = RunnableGraph.fromGraph(graph).run(materializer);
        future.onComplete(new OnComplete<Long>() {
            @Override
            public void onComplete(Throwable throwable, Long aLong) throws Throwable {
                system.terminate();
            }
        }, system.dispatcher());
    }

    public static void collectToFile(String sysName, String inPath, String outPath, TypeAndOperationResolver typeResolver,
                                     int maximumDistinctRecordKeys) throws IOException {
        final ActorSystem system = ActorSystem.create(sysName);
        final ActorMaterializer materializer = createMaterializer(system);

        Sink<ByteString, Future<Long>> fileSink = SynchronousFileSink.create(new File(outPath));
        Source<Future<Pair<String, Object>>, Future<Long>> futureSource = createSource(inPath, typeResolver, materializer);
        Future<Long> future = futureSource
                .buffer(maximumDistinctRecordKeys, OverflowStrategy.fail()) //an explicit failure instead of a silent deadlock
                .mapAsync(4, i -> i)
                .map(pair -> ByteString.fromString(String.format("%s;%s\n", pair.first(), pair.second())))
                .runWith(fileSink, materializer);

        future.onComplete(new OnComplete<Long>() {
            @Override
            public void onComplete(Throwable throwable, Long aLong) throws Throwable {
                system.terminate();
            }
        }, system.dispatcher());
    }

    @SuppressWarnings("unchecked")
    private static Source<Future<Pair<String, Object>>, Future<Long>> createSource(
            String inPath, TypeAndOperationResolver typeResolver, ActorMaterializer materializer) {

        final File inputFile = new File(inPath);

        return SynchronousFileSource.create(inputFile)
                .via(Framing.delimiter(ByteString.fromString(System.lineSeparator()), 512, true))
                .map(b -> {
                    Matcher matcher = dataPattern.matcher(b.utf8String());
                    if (!matcher.find()) {
                        throw new IllegalArgumentException("not parse ID");
                    }
                    return Pair.create(matcher.group(1), typeResolver.valueOf(matcher.group(2)));
                })
                .groupBy(Pair::first)
                .map(topPair -> Pair.create(topPair.first(), topPair.second().map(Pair::second)))
                .map(topPair -> topPair.second().runFold(
                        Pair.create(topPair.first(), typeResolver.empty()),
                        (pair, value) -> Pair.create(pair.first(), typeResolver.compute(pair.second(), value)), materializer));
    }

    private static ActorMaterializer createMaterializer(ActorSystem system) {
        Function<Throwable, Supervision.Directive> decider = ex -> {
            if (ex instanceof NumberFormatException) {
                log.error("NumberFormatException: " + ex.getMessage());
            }
            return Supervision.stop();
        };
        return ActorMaterializer.create(ActorMaterializerSettings.create(system)
                .withSupervisionStrategy(decider), system);
    }
}
