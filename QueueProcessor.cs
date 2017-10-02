using System;
using System.Net;
using System.Collections.Generic;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Threading.Tasks;
using BlackBarLabs.Extensions;
using System.Linq;
using BlackBarLabs.Collections.Generic;

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

        public void Execute()
        {
            receiveClient.OnMessageAsync(
                async (receivedMessage) =>
                {
                    try
                    {
                        var message = receivedMessage;
                        var waitToMarkTaskComplete = new System.Threading.AutoResetEvent(false);
                        var messageAction = MessageProcessStatus.NoAction;
                        var why = string.Empty;
                        var renewTask = Task.Run<Task>(
                            () =>
                            {
                                while (true)
                                {
                                    var fiveSecondsBeforeExpiration = (message.LockedUntilUtc - DateTime.UtcNow) - TimeSpan.FromSeconds(5);
                                    if (fiveSecondsBeforeExpiration.TotalMilliseconds <= 0 ||
                                        !waitToMarkTaskComplete.WaitOne(fiveSecondsBeforeExpiration))
                                    {
                                        try
                                        {
                                            message.RenewLock();
                                        }
                                        catch (Exception)
                                        {
                                            return 0.ToTask();
                                        }
                                        continue;
                                    }
                                    try
                                    {
                                        switch (messageAction)
                                        {
                                            case MessageProcessStatus.Complete:
                                                return message.CompleteAsync();
                                            case MessageProcessStatus.ReprocessLater:
                                            case MessageProcessStatus.ReprocessImmediately:
                                            case MessageProcessStatus.NoAction:
                                                return message.DeferAsync();
                                            case MessageProcessStatus.Broken:
                                                return message.DeadLetterAsync(why, why);
                                            default:
                                                return message.DeadLetterAsync("Unknown message action", "Unknown message action");
                                        }
                                    }
                                    catch (Exception)
                                    {
                                        return 0.ToTask();
                                    }
                                }
                            });

                        // Process the message
                        var messageParams = ParseMessageParams(message.Properties);
                        messageAction = await ProcessMessageAsync(messageParams,
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
                            (whyReturned) =>
                            {
                                why = whyReturned;
                                return MessageProcessStatus.Broken;
                            });
                        waitToMarkTaskComplete.Set();
                        await await renewTask;
                    }
                    catch (Exception ex)
                    {
                        var telemetryData = (new System.Collections.Generic.Dictionary<string, string>
                        {
                               { "receivedMessage.MessageId", receivedMessage.MessageId },
                               { "receivedMessage.SessionId", receivedMessage.SessionId },
                               { "receivedMessage.ContentType", receivedMessage.ContentType },
                               { "receivedMessage.SequenceNumber", receivedMessage.SequenceNumber.ToString() },
                               { "exception.Message", ex.Message },
                               { "exception.StackTrace", ex.StackTrace },
                        })
                        .Concat(receivedMessage.Properties
                            .Select(prop => $"receivedMessage.Properties[{prop.Key}]".PairWithValue(
                                null == prop.Value ? string.Empty : prop.Value.ToString())))
                        .ToDictionary();
                        //telemetry.TrackException(ex, telemetryData);
                    }
                }, GetOptions());
        }

        private static OnMessageOptions GetOptions()
        {
            var eventDrivenMessagingOptions = new OnMessageOptions();
            eventDrivenMessagingOptions.AutoComplete = false;
            eventDrivenMessagingOptions.ExceptionReceived += (object sender, ExceptionReceivedEventArgs e) =>
            {
                if (e != null && e.Exception != null)
                {
                    //telemetry.TrackTraceAndFlush(" > Exception received: " + e.Exception.Message);
                }
            };
            eventDrivenMessagingOptions.MaxConcurrentCalls = 10;
            return eventDrivenMessagingOptions;
        }

        public MessageProcessStatus Execute2()
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
            var message = new BrokeredMessage();
            EncodeMessageParams(message.Properties, messageParams);
            message.Properties[MESSAGE_PROPERTY_KEY_MESSAGE_NAME] = subscription;
            if (!sendClients.ContainsKey(subscription))
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
                }
                catch (System.UnauthorizedAccessException)
                {
                    // Swallow case where management permissions are not on connection string policy
                }
            }
            var sendClient = sendClients[subscription];
            lock (sendClient)
            {
                sendClient.Send(message);
            }
        }
    }
}