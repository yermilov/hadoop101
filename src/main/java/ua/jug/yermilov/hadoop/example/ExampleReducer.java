package ua.jug.yermilov.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yaroslav.yermilov
 */
public class ExampleReducer extends Reducer<Text, LongWritable, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long maxValue = Long.MIN_VALUE;

        for (LongWritable value : values) {
            if (value.get() > maxValue) {
                maxValue = value.get();
            }
        }

        String hours = Long.toString(maxValue / 100);
        String minutes = Long.toString(Math.round(maxValue % 100 * 0.6));
        if (minutes.length() == 1) minutes = "0" + minutes;

        outputValue.set(hours + ":" + minutes);

        context.write(key, outputValue);
    }
}
