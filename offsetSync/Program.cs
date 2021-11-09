using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Collections.Generic;


namespace offsetSync
{
    class Program
    {
        static void Main(string[] args)
        {
            // Uses admin client to find the partitions of the topic
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = "kafka:9092"
            };
            var adminBuilder = new AdminClientBuilder(adminConfig);
            using(var adminClient = adminBuilder.Build()) {
                    var meta = adminClient.GetMetadata(TimeSpan.FromDays(1));
                    var topic = meta.Topics.Find(t => t.Topic == "test");
                    var topicPartitions = topic.Partitions;
                    topicPartitions.ForEach( partition => Console.WriteLine($"Partition {partition.PartitionId}"));
            }
            var newConfig = new ConsumerConfig
            {
                GroupId = "newGroupId",
                BootstrapServers = "kafka:9092"
            };
            var oldConfig = new ConsumerConfig
            {
                GroupId = "oldGroupId",
                BootstrapServers = "kafka:9092"
            };

            // Uses Consumer to fetch the current offset
            // example only process 1 partitions, need to do it for all partitions
            long offset = 0;
            var builder = new ConsumerBuilder<string, string>(oldConfig);
            using (var consumer = builder.Build())
            {
                var parts = new List<TopicPartition>();
                parts.Add(new TopicPartition("test", 0));
                var res = consumer.Committed(parts, TimeSpan.FromDays(1));
                offset = res[0].Offset;
                Console.WriteLine($"Fetch offset: {offset}");
            }
            // New consumer builder to commit offset for another Consumer Group
            var builder2 = new ConsumerBuilder<string, string>(newConfig);
            using (var consumer = builder.Build())
            {
                var parts = new List<TopicPartitionOffset>();
                parts.Add(new TopicPartitionOffset(new TopicPartition("test", 0), offset));
                consumer.Commit(parts);
                Console.WriteLine("Offset committed to new consumer group");
            }
        }
    }
}
