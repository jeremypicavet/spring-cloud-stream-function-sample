**A simple spring boot project that reproduces issue described here : https://github.com/spring-cloud/spring-cloud-stream/issues/1726**

The goal is to convert each line of a csv file into message (published on rabbitmq) using a converter provided by a spring cloud function :

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
  
We used our own integration flow (inspired by the project "spring-cloud-starter-stream-source-file") :

    @Bean
    public IntegrationFlow fileSourceFlow() {
        FileInboundChannelAdapterSpec messageSourceSpec = Files.inboundAdapter(new File(this.properties.getDirectory()));
        messageSourceSpec.preventDuplicates(this.properties.isPreventDuplicates());

        IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(messageSourceSpec)
                .enrichHeaders(Collections.<String, Object>singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
                .split(new FileSplitter(false));

        return flowBuilder.channel(source.output()).get();
    }



When dealing with files of few lines, it works as expected, even if an exception is thrown during the conversion. 

As soon as we provide a file containing hundreds of lines, the stream stop if the file contains a conversion error.

**We provide 3 test files under "all_files/" directory :**

* **file_without_conversion_error.csv** : 1000 lines, does not contain conversion error, it will be fully processed as expected
* **file_with_one_conversion_error.csv** : 1000 lines, contains an error (conversion string -> integer) on the 500th line, an exception is thrown and causing an unexpecting stop ...
* **file_with_few_lines_and_one_conversion_error.csv** : 4 lines, contains an error on the second line, 3 lines will be fully processed, and the line containing the conversion error will be redirected on error channel as expected.

**==> Move one of these files under "files/" directory for processing**


**Run application :**

1. First, we need to provide rabbitmq configuration :

```
spring.rabbitmq.host=
spring.rabbitmq.port=
spring.rabbitmq.username=
spring.rabbitmq.password=
```

2. and launch bootRun task :

```
./gradlew bootRun
```

**Analysis :**


The backpressure error is thrown in method **FluxConcatMap.ConcatMapImmediate.onNext(T t)** :

```
			else if (!queue.offer(t)) { // queue.offer(t) returns true !
				onError(Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						this.ctx));
				Operators.onDiscard(t, this.ctx);
			}
```

This causes a call to method **AbstractSubscribableChannel.unsubscribe(MessageHandler handler)**, so subsequent messages published on output channel won't be processed :

```
2019-06-06 15:11:53.753 ERROR 9896 --- [ask-scheduler-1] o.s.integration.handler.LoggingHandler   : org.springframework.messaging.MessageDeliveryException: Dispatcher has no subscribers for channel 'DemoApp.output'.; nested exception is org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=123;123;123, headers={sequenceNumber=534, file_name=file_with_one_conversion_error.csv, sequenceSize=0, correlationId=619008f3-28f2-5957-52ba-d58ec53d5cb4, file_originalFile=.\files\file_with_one_conversion_error.csv, id=411e1e71-e844-d72a-46d0-8b0c2d2c5d2d, contentType=text/plain, file_relativePath=file_with_one_conversion_error.csv, timestamp=1559826712290}], failedMessage=GenericMessage [payload=123;123;123, headers={sequenceNumber=534, file_name=file_with_one_conversion_error.csv, sequenceSize=0, correlationId=619008f3-28f2-5957-52ba-d58ec53d5cb4, file_originalFile=.\files\file_with_one_conversion_error.csv, id=411e1e71-e844-d72a-46d0-8b0c2d2c5d2d, contentType=text/plain, file_relativePath=file_with_one_conversion_error.csv, timestamp=1559826712290}]
        at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:77)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
        at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:431)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.doProduceOutput(AbstractMessageProducingHandler.java:284)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:265)
        at org.springframework.integration.splitter.AbstractMessageSplitter.produceOutput(AbstractMessageSplitter.java:246)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:223)
        at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:129)
        at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:162)
        at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115)
        at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:132)
        at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
        at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
        at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:431)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.doProduceOutput(AbstractMessageProducingHandler.java:284)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:265)
        at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:223)
        at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:129)
        at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:162)
        at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115)
        at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:132)
        at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
        at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
        at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
        at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
        at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
        at org.springframework.integration.endpoint.SourcePollingChannelAdapter.handleMessage(SourcePollingChannelAdapter.java:234)
        at org.springframework.integration.endpoint.AbstractPollingEndpoint.doPoll(AbstractPollingEndpoint.java:390)
        at org.springframework.integration.endpoint.AbstractPollingEndpoint.pollForMessage(AbstractPollingEndpoint.java:329)
        at org.springframework.integration.endpoint.AbstractPollingEndpoint.lambda$null$1(AbstractPollingEndpoint.java:277)
        at org.springframework.integration.util.ErrorHandlingTaskExecutor.lambda$execute$0(ErrorHandlingTaskExecutor.java:57)
        at org.springframework.core.task.SyncTaskExecutor.execute(SyncTaskExecutor.java:50)
        at org.springframework.integration.util.ErrorHandlingTaskExecutor.execute(ErrorHandlingTaskExecutor.java:55)
        at org.springframework.integration.endpoint.AbstractPollingEndpoint.lambda$createPoller$2(AbstractPollingEndpoint.java:274)
        at org.springframework.scheduling.support.DelegatingErrorHandlingRunnable.run(DelegatingErrorHandlingRunnable.java:54)
        at org.springframework.scheduling.concurrent.ReschedulingRunnable.run(ReschedulingRunnable.java:93)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:748)
Caused by: org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=123;123;123, headers={sequenceNumber=534, file_name=file_with_one_conversion_error.csv, sequenceSize=0, correlationId=619008f3-28f2-5957-52ba-d58ec53d5cb4, file_originalFile=.\files\file_with_one_conversion_error.csv, id=411e1e71-e844-d72a-46d0-8b0c2d2c5d2d, contentType=text/plain, file_relativePath=file_with_one_conversion_error.csv, timestamp=1559826712290}]
        at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:138)
        at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
        at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
        ... 56 more
```

Stacktrace snapshot of the unsubscribe call :

```
"task-scheduler-1@8024" prio=5 tid=0x15 nid=NA runnable
  java.lang.Thread.State: RUNNABLE
	  at org.springframework.integration.channel.AbstractSubscribableChannel.unsubscribe(AbstractSubscribableChannel.java:56)
	  at org.springframework.integration.channel.MessageChannelReactiveUtils$SubscribableChannelPublisherAdapter.lambda$null$0(MessageChannelReactiveUtils.java:86)
	  at org.springframework.integration.channel.MessageChannelReactiveUtils$SubscribableChannelPublisherAdapter$$Lambda$731.629289362.dispose(Unknown Source:-1)
	  at reactor.core.publisher.FluxCreate$SinkDisposable.cancel(FluxCreate.java:1039)
	  at reactor.core.publisher.FluxCreate$BaseSink.disposeResource(FluxCreate.java:472)
	  at reactor.core.publisher.FluxCreate$BaseSink.cancel(FluxCreate.java:461)
	  at reactor.core.publisher.Operators.onOperatorError(Operators.java:604)
	  at reactor.core.publisher.FluxConcatMap$ConcatMapImmediate.onNext(FluxConcatMap.java:239)
	  at reactor.core.publisher.FluxCreate$IgnoreSink.next(FluxCreate.java:618)
	  at reactor.core.publisher.FluxCreate$SerializedSink.next(FluxCreate.java:153)
	  at org.springframework.integration.channel.MessageChannelReactiveUtils$SubscribableChannelPublisherAdapter$$Lambda$730.1824259625.handleMessage(Unknown Source:-1)
	  at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:132)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
	  at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
	  at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:431)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.doProduceOutput(AbstractMessageProducingHandler.java:284)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:265)
	  at org.springframework.integration.splitter.AbstractMessageSplitter.produceOutput(AbstractMessageSplitter.java:246)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:223)
	  at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:129)
	  at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:162)
	  at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:132)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
	  at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
	  at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:431)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.doProduceOutput(AbstractMessageProducingHandler.java:284)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:265)
	  at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:223)
	  at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:129)
	  at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:162)
	  at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:115)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:132)
	  at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:105)
	  at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:73)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:453)
	  at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:401)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
	  at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
	  at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:109)
	  at org.springframework.integration.endpoint.SourcePollingChannelAdapter.handleMessage(SourcePollingChannelAdapter.java:234)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint.doPoll(AbstractPollingEndpoint.java:390)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint$$Lambda$735.511507785.call(Unknown Source:-1)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint.pollForMessage(AbstractPollingEndpoint.java:329)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint.lambda$null$1(AbstractPollingEndpoint.java:277)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint$$Lambda$737.49388452.run(Unknown Source:-1)
	  at org.springframework.integration.util.ErrorHandlingTaskExecutor.lambda$execute$0(ErrorHandlingTaskExecutor.java:57)
	  at org.springframework.integration.util.ErrorHandlingTaskExecutor$$Lambda$738.204891530.run(Unknown Source:-1)
	  at org.springframework.core.task.SyncTaskExecutor.execute(SyncTaskExecutor.java:50)
	  at org.springframework.integration.util.ErrorHandlingTaskExecutor.execute(ErrorHandlingTaskExecutor.java:55)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint.lambda$createPoller$2(AbstractPollingEndpoint.java:274)
	  at org.springframework.integration.endpoint.AbstractPollingEndpoint$$Lambda$736.1172738852.run(Unknown Source:-1)
	  at org.springframework.scheduling.support.DelegatingErrorHandlingRunnable.run(DelegatingErrorHandlingRunnable.java:54)
	  at org.springframework.scheduling.concurrent.ReschedulingRunnable.run(ReschedulingRunnable.java:93)
	  at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	  at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	  at java.lang.Thread.run(Thread.java:748)
```

