package ua.jug.yermilov.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yaroslav.yermilov
 */
public class ExampleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static SimpleDateFormat INPUT_FORMAT_1 = new SimpleDateFormat("dd/MM/yyyy");
    private final static SimpleDateFormat INPUT_FORMAT_2 = new SimpleDateFormat("dd. MM. yyyy");
    private final static SimpleDateFormat OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM");

    private final Text outputKey = new Text();
    private final LongWritable outputValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            if (!line.startsWith("Id")) {
                String[] columns = line.split(",");

                String textDate = columns[2];
                String textHours = columns[5];

                String month;

                if (textDate.contains("/")) {
                    String justDate = textDate.substring(0, textDate.indexOf(' '));
                    Date date = INPUT_FORMAT_1.parse(justDate);
                    month = OUTPUT_FORMAT.format(date);
                } else {
                    String justDate = textDate.substring(0, textDate.lastIndexOf(' '));
                    Date date = INPUT_FORMAT_2.parse(justDate);
                    month = OUTPUT_FORMAT.format(date);
                }

                Long hours = Math.round(Double.parseDouble(textHours) * 100);

                outputKey.set(month);
                outputValue.set(hours);

                context.write(outputKey, outputValue);
            }
        } catch (Exception e) { }
    }
}
