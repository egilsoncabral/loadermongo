package com.challenge.batch;

import com.challenge.listerners.JobCompletionNotificationListener;
import com.challenge.listerners.StepExecutionNotificationListener;
import com.challenge.model.VehicleDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.File;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableBatchProcessing
public class DataProcessJob extends DefaultBatchConfigurer {

    private static final Logger log = LoggerFactory.getLogger(DataProcessJob.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value("${input.file.name}")
    private static String fileName;


    @Bean
    public JobCompletionNotificationListener jobExecutionListener() {
        return new JobCompletionNotificationListener();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor t = new SimpleAsyncTaskExecutor();
        t.setConcurrencyLimit(10);
        return t;
    }


    @Bean
    public StepExecutionNotificationListener stepExecutionListener() {
        return new StepExecutionNotificationListener();
    }

    @Bean
    public Job readCSVFile() {
        return jobBuilderFactory.get("readCSVFile")
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener())
                .flow(step()).end().build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get("step").<VehicleDetail, VehicleDetail>chunk(8)
                .reader(reader())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .listener(stepExecutionListener())
                .throttleLimit(10).build();
    }

    @Bean
    public FlatFileItemReader<VehicleDetail> reader() {
        FlatFileItemReader<VehicleDetail> reader = new FlatFileItemReader<>();
        File file = new File(fileName);
        reader.setResource(new ClassPathResource("bus_data.csv"));
        reader.setLineMapper(new DefaultLineMapper<VehicleDetail>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"timestamp", "lineId",
                        "direction", "journeyPatternId", "timeFrame", "vehicleJourneyId", "operator",
                        "congestion", "longitude", "latitude", "delay", "blockId", "vehicleId", "stopId", "atStop"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<VehicleDetail>() {{
                setTargetType(VehicleDetail.class);
            }});
        }});
        return reader;
    }

    @Bean
    public MongoItemWriter<VehicleDetail> writer() {
        MongoItemWriter<VehicleDetail> writer = new MongoItemWriter<VehicleDetail>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("vehicle");
        return writer;
    }

    public static void main(String[] args) {
        fileName = args[0];
        if (fileName == null){
            log.error("Please, inform the file name.");
        }
        long time = System.currentTimeMillis();
        SpringApplication.run(DataProcessJob.class, args);
        time = System.currentTimeMillis() - time;
        log.info("Runtime: {} seconds.", ((double) time / 1000));
    }

}
