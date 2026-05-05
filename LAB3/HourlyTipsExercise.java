package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, 60, 600)));

        // Использование reduce + processWindowFunction для эффективности      DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .timeWindow(Time.hours(1))
                .reduce(
                    (f1, f2) -> { f1.tip += f2.tip; return f1; }, // Инкрементальная сумма
                    new WrapResult()
                );

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(2);

        printOrTest(hourlyMax);
        env.execute("Hourly Tips");
    }

    public static class WrapResult extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<TaxiFare> aggregatedFare, Collector<Tuple3<Long, Long, Float>> out) {
            TaxiFare result = aggregatedFare.iterator().next();
            out.collect(new Tuple3<>(context.window().getEnd(), key, result.tip));
        }
    }
}