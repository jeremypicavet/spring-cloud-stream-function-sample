This sample project reproduces issue describe here : https://github.com/spring-cloud/spring-cloud-stream/issues/1726

The goal is to convert each line of a csv file into message (published on rabbitmq here) using a converter provided by a spring cloud function :

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
  
Source used in this sample is an **IntegrationFlow** provided by the project "spring-cloud-starter-stream-source-file".

When dealing with files of few lines, it works as expected, even if an exception is thrown during the conversion, . 

As soon as we provide a file containing hundreds of lines (thousand in our sample), an exception is thrown and the stream stop.


**Run application :**

./gradlew bootRun

**We provide 3 files in this sample under "files/" directory :**

./files/file_without_conversion_error.csv : 1000 lines, does not contain conversion error, it will be fully processed as expected
./files/file_with_one_conversion_error.csv : 1000 lines, contains an error (conversion string -> integer) in the middle of the file, an exception is thrown and causing an unexpecting stop ...
./files/file_with_few_lines_and_one_conversion_error.csv : 4 lines, containes an error on the second lines, 3 lines will be fully processed, and the line containing the conversion error will be redirected on error channel as expected.




