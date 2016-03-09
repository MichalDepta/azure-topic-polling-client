using System;
using System.Collections.Generic;
using Microsoft.Azure;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusTopicConsumer
{
    public class Program
    {
        private const string TopicName = "TestTopic";
        private const string SubscriptionName = "TestSubscription";

        public static void Main(string[] args)
        {
            var client = CreateSubscriptionClient();
            var producer = CreateTopicClient();
            Console.WriteLine("Enter messages to process:");

            try
            {
                while (true)
                {
                    var line = Console.ReadLine();

                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    if (ShouldQuit(line))
                    {
                        break;
                    }

                    if (MatchWithTopicItem(line, client, producer))
                    {
                        Console.WriteLine("Found a matching message");
                    }
                    else
                    {
                        Console.WriteLine("Could not find a matching message");
                    }
                }
            }
            finally
            {
                client.Close();
                producer.Close();
            }
        }

        private static bool ShouldQuit(string input) => "quit".Equals(input, StringComparison.OrdinalIgnoreCase);

        private static bool MatchWithTopicItem(string input, SubscriptionClient client, TopicClient producer)
        {
            var visitedMessages = new List<string>();
            var result = false;

            while (true)
            {
                var message = client.Receive(TimeSpan.FromSeconds(5));
                if (message == null)
                {
                    break;
                }

                var content = (string)message.Properties["content"];

                if (visitedMessages.Contains(content))
                {
                    message.Abandon();
                    break;
                }

                if (content.Equals(input))
                {
                    result = true;
                }
                else
                {
                    producer.Send(message.Clone());
                }

                message.Complete();

                visitedMessages.Add(content);
            }

            return result;
        }
        
        private static SubscriptionClient CreateSubscriptionClient()
        {
            var connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(TopicName))
            {
                namespaceManager.CreateTopic(TopicName);
            }

            if (!namespaceManager.SubscriptionExists(TopicName, SubscriptionName))
            {
                namespaceManager.CreateSubscription(TopicName, SubscriptionName);
            }

            return SubscriptionClient.CreateFromConnectionString(connectionString, TopicName, SubscriptionName, ReceiveMode.PeekLock);
        }

        private static TopicClient CreateTopicClient()
        {
            var connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(TopicName))
            {
                namespaceManager.CreateTopic(TopicName);
            }

            return TopicClient.CreateFromConnectionString(connectionString, TopicName);
        }
    }
}
