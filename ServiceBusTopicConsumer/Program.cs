using System;
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
            Console.WriteLine("Enter messages to process:");

            client.OnMessage(message =>
            {
                var content = message.Properties["content"];
                Console.WriteLine($"=> {content}");
            }, new OnMessageOptions {AutoComplete = false});

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

                    MatchWithTopicItem(line, client);
                }
            }
            finally
            {
                client.Close();
            }
        }

        private static bool ShouldQuit(string input) => "quit".Equals(input, StringComparison.OrdinalIgnoreCase);

        private static void MatchWithTopicItem(string input, SubscriptionClient client)
        {
            for (var i = 0;; i++)
            {
                var message = client.Peek(i);

                if (message == null)
                {
                    break;
                }

                var content = message.Properties["content"];

                if (content.Equals(input))
                {
                    // The following raises an InvalidOperationException:
                    message.Complete(); 
                    break;
                }
            }
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
    }
}
