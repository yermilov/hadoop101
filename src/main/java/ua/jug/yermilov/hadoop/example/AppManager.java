package ua.jug.yermilov.hadoop.example;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource("classpath:hadoop-context.xml")
public class AppManager implements CommandLineRunner {

    private final static Path HDFS_DATA_PATH = new Path("hdfs:///user/psgetl/data");
    private final static Path LOCAL_DATA_PATH = new Path("file:///home/psgetl/data.csv");
    private final static Path HDFS_OUTPUT_PATH = new Path("hdfs:///user/psgetl/output");

    @Autowired
    private Configuration configuration;

    @Autowired
    private Job job;

    public static void main(String[] args) throws Exception {
        new SpringApplication(AppManager.class).run(args);
    }
    
    public void run(String[] args) throws Exception {
        stageDataInHdfs();
        runMapReduce();
        printResult();
    }

    private void stageDataInHdfs() throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(HDFS_DATA_PATH)) {
            fs.delete(HDFS_DATA_PATH, true);
        }
        fs.mkdirs(HDFS_DATA_PATH);

        fs.copyFromLocalFile(false, true, LOCAL_DATA_PATH, HDFS_DATA_PATH);
    }

    private void runMapReduce() throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(HDFS_OUTPUT_PATH)) {
            fs.delete(HDFS_OUTPUT_PATH, true);
        }

        job.waitForCompletion(true);
    }

    private void printResult() throws Exception {
        FileSystem fs = FileSystem.get(configuration);

        FileStatus[] fileStatuses = fs.listStatus(HDFS_OUTPUT_PATH);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(IOUtils.toString(fs.open(fileStatus.getPath())));
        }
    }
}
