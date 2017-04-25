using System;
using System.Net;
using System.Collections.Generic;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Threading.Tasks;

namespace EastFive.Messaging
{
    public abstract class QueueProcessor<TMessageParam>
    {
        private QueueClient receiveClient;
        private static Dictionary<string, QueueClient> sendClients =
            new Dictionary<string, QueueClient>();

        private const string MESSAGE_PROPERTY_KEY_MESSAGE_NAME = "MessageName";
        
        protected QueueProcessor(string subscription)
        {
            var serviceBusConnectionString = Web.Configuration.Settings.Get(
                Configuration.MessageBusDefinitions.ServiceBusConnectionString);
            sendClients[subscription] = QueueClient.CreateFromConnectionString(serviceBusConnectionString, subscription);
            
            //// Create the topic if it does not exist already
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(serviceBusConnectionString);
            try
            {
                if (!namespaceManager.QueueExists(subscription))
                    namespaceManager.CreateQueue(subscription);
            } catch(System.UnauthorizedAccessException)
            {
                // Swallow case where management permissions are not on connection string policy
            }
            receiveClient =
                QueueClient.CreateFromConnectionString
                        (serviceBusConnectionString, subscription);
        }

        private static string GetSubscription()
        {
            return typeof(TMessageParam).FullName.Replace('.', '_');
        }

        private static string GetTopic(Type topic)
        {
            return topic.FullName.Replace('.', '_');
        }

        public enum MessageProcessStatus
        {
            Complete, ReprocessLater, ReprocessImmediately, Broken, NoAction,
        }

        protected virtual TMessageParam ParseMessageParams(IDictionary<string, object> messageParamsDictionary)
        {
            var constructorInfo = typeof(TMessageParam).GetConstructor(new Type[] { });
            TMessageParam messageParams = (TMessageParam)constructorInfo.Invoke(new object[] { });
            foreach (var propertyName in messageParamsDictionary.Keys)
            {
                var propertyInfo = typeof(TMessageParam).GetProperty(propertyName);
                if (propertyInfo != null) // Ignore extra fields at get stuffed into properties like "MessageName"
                {
                    var propertyValue = messageParamsDictionary[propertyName];
                    propertyInfo.SetValue(messageParams, propertyValue);
                }
            }
            return messageParams;
        }

        protected static void EncodeMessageParams(IDictionary<string, object> messageParamsDictionary, TMessageParam messageParams)
        {
            foreach (var propertyInfo in typeof(TMessageParam).GetProperties())
            {
                var propertyName = propertyInfo.Name;
                var propertyValue = propertyInfo.GetValue(messageParams);
                messageParamsDictionary[propertyName] = propertyValue;
            }
        }

        protected abstract Task<TResult> ProcessMessageAsync<TResult>(TMessageParam messageParams,
            Func<TResult> onProcessed,
            Func<TResult> onUnprocessed,
            Func<string, TResult> onBrokenMessage);

        public MessageProcessStatus Execute()
        {
            BrokeredMessage message;
            try
            {
                message = receiveClient.Receive();
            }
            catch (MessagingCommunicationException ex)
            {
                return MessageProcessStatus.NoAction;
            }

            if (message == null)
                return MessageProcessStatus.NoAction;
            try
            {
                var messageParams = ParseMessageParams(message.Properties);
                var result = ProcessMessageAsync(messageParams,
                    () =>
                    {
                        message.Complete();
                        return MessageProcessStatus.Complete;
                    },
                    () =>
                    {
                            // TODO: Update resent count and send error if it gets too large
                            // Let message get resent
                            return MessageProcessStatus.ReprocessImmediately;
                    },
                    (why) =>
                    {
                        TerminateMessage(message, why);
                        return MessageProcessStatus.Broken;
                    });
                return result.Result;
            }
            catch (WebException)
            {
                // Let message get resent
                return MessageProcessStatus.ReprocessLater;
            }
            catch (Exception)
            {
                // Indicate a problem, unlock message in subscription
                try
                {
                    // message.Complete();
                }
                catch (Exception)
                {
                    message.Abandon();
                }
                return MessageProcessStatus.Broken;
            }
        }

        private void TerminateMessage(BrokeredMessage message, string why)
        {
            try
            {
                message.DeadLetter(why, why);
            } catch (Exception)
            {
                message.Abandon();
            }
        }

        protected static void Send<TQueueProcessor>(TMessageParam messageParams, string subscription)
            where TQueueProcessor : QueueProcessor<TMessageParam>
        {
            //var topic = GetTopic(typeof(TQueueProcessor));
            var message = new BrokeredMessage();
            EncodeMessageParams(message.Properties, messageParams);
            message.Properties[MESSAGE_PROPERTY_KEY_MESSAGE_NAME] = subscription;
            if (!sendClients.ContainsKey(subscription))
            {
                var serviceBusConnectionString = Web.Configuration.Settings.Get(
                    Configuration.MessageBusDefinitions.ServiceBusConnectionString);
                sendClients[subscription] = QueueClient.CreateFromConnectionString(
                    serviceBusConnectionString, subscription);
            }
            var sendClient = sendClients[subscription];
            lock (sendClient)
            {
                sendClient.Send(message);
            }
        }
    }
}