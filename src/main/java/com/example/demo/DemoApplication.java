package com.example.demo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.app.file.source.FileSourceProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.dsl.FileInboundChannelAdapterSpec;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.splitter.FileSplitter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;

import java.io.File;
import java.util.Collections;
import java.util.function.Function;

@SpringBootApplication
@EnableBinding(Source.class)
@EnableConfigurationProperties({ FileSourceProperties.class })
public class DemoApplication {

    @Autowired
    private FileSourceProperties properties;

    @Autowired
    private Source source;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public IntegrationFlow fileSourceFlow() {
        FileInboundChannelAdapterSpec messageSourceSpec = Files.inboundAdapter(new File(this.properties.getDirectory()));
        messageSourceSpec.preventDuplicates(this.properties.isPreventDuplicates());

        IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(messageSourceSpec)
                .enrichHeaders(Collections.<String, Object>singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
                .split(new FileSplitter(false));

        return flowBuilder.channel(source.output()).get();
    }

    @Bean
    public Function<String, MyPojo> myConverter() {
        return csvLine -> {
            try {
                return csvObjectReader().readValue(csvLine);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @StreamListener("errorChannel")
    public void error(Message<?> message) {
        System.out.println("Handling ERROR: " + message);
    }

    @Bean
    public ObjectReader csvObjectReader() {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = mapper.schemaFor(MyPojo.class).withColumnSeparator(';').withNullValue("NULL").withoutHeader();
        mapper.findAndRegisterModules();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readerFor(MyPojo.class).with(schema);
    }
}
