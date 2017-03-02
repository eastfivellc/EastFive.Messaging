﻿using System;
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
        private static QueueClient errorClient;

        private const string MESSAGE_PROPERTY_KEY_MESSAGE_NAME = "MessageName";
        private const string ERROR_TOPIC = "ERRORS";
        
        protected QueueProcessor(string subscription)
        {
            var serviceBusConnectionString = Web.Configuration.Settings.Get(
                Configuration.MessageBusDefinitions.ServiceBusConnectionString);
            sendClients[subscription] = QueueClient.CreateFromConnectionString(serviceBusConnectionString, subscription);
            errorClient = QueueClient.CreateFromConnectionString(serviceBusConnectionString, ERROR_TOPIC);

            //// Create the topic if it does not exist already
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(serviceBusConnectionString);
            //if (!namespaceManager.TopicExists(topic))
            //{
            //    try
            //    {
            //        namespaceManager.CreateTopic(topic);
            //    }
            //    catch (ArgumentException ex)
            //    {
            //        if (!(ex.InnerException is WebException))
            //            throw;
            //        var webEx = ex.InnerException as WebException;
            //        if (!(webEx.Response is HttpWebResponse))
            //            throw;
            //        var httpResponse = webEx.Response as HttpWebResponse;
            //        if (httpResponse.StatusCode != HttpStatusCode.BadRequest)
            //            throw;
            //        // It's just free tier and topics aren't allowed.
            //    }
            //}
            //if (!namespaceManager.SubscriptionExists(topic, subscription))
            //{
            //    var applicationInstanceFilter = new SqlFilter ($"{MESSAGE_PROPERTY_KEY_MESSAGE_NAME} = '{subscription}'");
            //    namespaceManager.CreateSubscription(topic, subscription, applicationInstanceFilter);
            //    namespaceManager.CreateSubscription(new SubscriptionDescription()
            //    {

            //    });
            //}
            if (!namespaceManager.QueueExists(subscription))
                namespaceManager.CreateQueue(subscription);
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
            // Send an error message before sticking it on the dead letter queue
            var errorMessage = new BrokeredMessage();
            errorMessage.Properties.Add("message_id", message.MessageId);

            // Add the error data conservatively converting everything to strings
            //foreach (var errorDataKey in ex.Data.Keys)
            //{
            //    var errorDataValue = ex.Data[errorDataKey];
            //    errorMessage.Properties.Add(errorDataKey.ToString(), errorDataValue.ToString());
            //}

            // Serialze other collections
            var stream = new System.IO.MemoryStream();

            // store message properties
            new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(IDictionary<string, object>)).WriteObject(stream, message.Properties);
            var messageProperties = System.Text.Encoding.UTF8.GetString(stream.GetBuffer());
            errorMessage.Properties.Add("message_properties", messageProperties);

            // store inner exception
            //try
            //{
            //    new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(Exception)).WriteObject(stream, ex.InnerException);
            //    var innerException = System.Text.Encoding.UTF8.GetString(stream.GetBuffer());
            //    errorMessage.Properties.Add("inner_exception", innerException);
            //}
            //catch (Exception)
            //{
            //    // This is optional anyway
            //}

            lock (errorClient)
            {
                errorClient.Send(errorMessage);
            }

            message.DeadLetter(why, errorMessage.MessageId);
        }

        protected static void Send<TQueueProcessor>(TMessageParam messageParams, string subscription)
            where TQueueProcessor : QueueProcessor<TMessageParam>
        {
            //var topic = GetTopic(typeof(TQueueProcessor));
            var message = new BrokeredMessage();
            EncodeMessageParams(message.Properties, messageParams);
            message.Properties[MESSAGE_PROPERTY_KEY_MESSAGE_NAME] = subscription;
            var sendClient = sendClients[subscription];
            lock (sendClient)
            {
                sendClient.Send(message);
            }
        }
    }
}