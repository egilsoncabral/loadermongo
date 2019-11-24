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
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
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
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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

    @Value("file:${input.file.name}")
    private Resource resourceFile;


    @Bean
    public JobCompletionNotificationListener jobExecutionListener() {
        return new JobCompletionNotificationListener();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor t = new SimpleAsyncTaskExecutor();
        t.setConcurrencyLimit(90);
        return t;
    }


    @Bean
    public StepExecutionNotificationListener stepExecutionListener() {
        return new StepExecutionNotificationListener();
    }

    @Bean
    public Job readCSVFile() {
        Job job = null;
        if (resourceFile != null && resourceFile.exists()){
            job = jobBuilderFactory.get("readCSVFile")
                    .incrementer(new RunIdIncrementer())
                    .start(splitFlow())
                    .build()
                    .build();
        } else {
            log.error("File not found!");
            log.error("Please, inform the file name as params such as -Dinput.file.name=xxxx.csv.");
        }
        return job;
    }


    @Bean
    public Flow splitFlow() {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(taskExecutor())
                .add(flow1(), flow2(), flow3(), flow4(), flow5(), flow6(), flow7(), flow8(), flow9(), flow10())
                .build();
    }

    @Bean
    public Flow flow1() {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow2() {
        return new FlowBuilder<SimpleFlow>("flow2")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow3() {
        return new FlowBuilder<SimpleFlow>("flow3")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow4() {
        return new FlowBuilder<SimpleFlow>("flow4")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow5() {
        return new FlowBuilder<SimpleFlow>("flow5")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow6() {
        return new FlowBuilder<SimpleFlow>("flow6")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow7() {
        return new FlowBuilder<SimpleFlow>("flow7")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow8() {
        return new FlowBuilder<SimpleFlow>("flow8")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow9() {
        return new FlowBuilder<SimpleFlow>("flow9")
                .start(step())
                .build();
    }

    @Bean
    public Flow flow10() {
        return new FlowBuilder<SimpleFlow>("flow10")
                .start(step())
                .build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get("step").<VehicleDetail, VehicleDetail>chunk(10)
                .reader(reader())
                .writer(writer())
                .listener(jobExecutionListener())
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<VehicleDetail> reader(){
        FlatFileItemReader<VehicleDetail> reader = new FlatFileItemReader<>();
        reader.setResource(resourceFile);
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
        long time = System.currentTimeMillis();
        SpringApplication.run(DataProcessJob.class, args);
        time = System.currentTimeMillis() - time;
        log.info("Runtime: {} seconds.", ((double) time / 1000));
    }

}
