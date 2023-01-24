using Confluent.Kafka;

namespace dotNetTest;

internal static class ReadFromBeginning
{
    static void Main(string[] args)
    {
        var topicName = "test";
        var bootstrap = "kafka:9092";

        // Uses admin client to find the partitions of the topic
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrap
        };
        var parts = new List<TopicPartition>();
        var adminBuilder = new AdminClientBuilder(adminConfig);
        using (var adminClient = adminBuilder.Build())
        {
            var topicMeta = adminClient.GetMetadata(topicName, TimeSpan.FromDays(1));
            var topicPartitions = topicMeta.Topics[0].Partitions;
            Console.WriteLine($"Number of partition: {topicPartitions.Count}");
            foreach (var topicPartition in topicPartitions)
            {
                parts.Add(new TopicPartition(topicName, topicPartition.PartitionId));
            }
        }

        var consumerConfig = new ConsumerConfig
        {
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // the group.id property must be specified when creating a consumer, even 
            // if you do not intend to use any consumer group functionality.
            GroupId = "Dummy",
            BootstrapServers = bootstrap,
            EnableAutoCommit = false,
            IsolationLevel = IsolationLevel.ReadUncommitted,
            EnablePartitionEof = true,
            // Debug = "consumer,cgrp,topic,fetch"
            Debug = "protocol,msg"
        };


        var builder = new ConsumerBuilder<string, string>(consumerConfig);
        using (var consumer = builder
                   .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                   .SetLogHandler((_, l) => Console.WriteLine($"Log: {l.Name} {l.Message}"))
                   .Build())
        {
            var topicPartitionOffset = new List<TopicPartitionOffset>();
            foreach (var tp in parts)
            {
                var watermarkOffsets = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromDays(1));
                Console.WriteLine($"Partition: {tp.Partition.Value} start: {watermarkOffsets.Low} end: {watermarkOffsets.High}");
                topicPartitionOffset.Add(new TopicPartitionOffset(tp, watermarkOffsets.Low));
            }
            consumer.Assign(topicPartitionOffset);
            var eofPartitionCount = parts.Count;
            while (eofPartitionCount > 0)
            {
                var consumeResult = consumer.Consume();
                if (consumeResult.IsPartitionEOF)
                {
                    eofPartitionCount--;
                }
                else
                {
                    var partitionId = consumeResult.Partition.Value;
                    var offset = consumeResult.Offset.Value;
                    Console.WriteLine(
                        $"Read from partition={partitionId} offset={offset} key={consumeResult.Message.Key} value={consumeResult.Message.Value}");
                }
            }
        }
    }
}