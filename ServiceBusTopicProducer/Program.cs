using System;
using Microsoft.Azure;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusTopicProducer
{
    public class Program
    {
        private const string TopicName = "TestTopic";
        private const string SubscriptionName = "TestSubscription";

        public static void Main(string[] args)
        {
            var client = CreateTopicClient();

            Console.WriteLine("Enter messages to send:");

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

                    var message = CreateMessage(line);
                    client.Send(message);
                }
            }
            finally
            {
                client.Close();
            }
        }

        private static bool ShouldQuit(string input) => "quit".Equals(input, StringComparison.OrdinalIgnoreCase);

        private static BrokeredMessage CreateMessage(string content)
        {
            var message = new BrokeredMessage();
            message.Properties["content"] = content;
            return message;
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
