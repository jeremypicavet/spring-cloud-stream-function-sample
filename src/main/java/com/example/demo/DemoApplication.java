package com.example.demo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.app.file.source.FileSourceConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;

import java.util.function.Function;

@SpringBootApplication
@Import(FileSourceConfiguration.class)
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public Function<String, MyPojo> myConverter(){
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
	public ObjectReader csvObjectReader(){
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = mapper.schemaFor(MyPojo.class).withColumnSeparator(';').withNullValue("NULL").withoutHeader();
		mapper.findAndRegisterModules();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper.readerFor(MyPojo.class).with(schema);
	}

}
