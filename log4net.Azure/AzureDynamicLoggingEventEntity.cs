using System;
using System.Collections;
using log4net.Core;

namespace log4net.Appender
{
    internal sealed class AzureDynamicLoggingEventEntity : ElasticTableEntity
    {
        public AzureDynamicLoggingEventEntity(LoggingEvent e, PartitionKeyTypeEnum partitionKeyType)
        {
            this["Domain"] = e.Domain;
            this["Identity"] = e.Identity;
            this["Level"] = e.Level.ToString();
            this["LoggerName"] = e.LoggerName;
            this["Message"] = LimitEntryLength(e.RenderedMessage + Environment.NewLine + e.GetExceptionString());
            this["EventTimeStamp"] = e.TimeStamp;
            this["ThreadName"] = e.ThreadName;
            this["UserName"] = e.UserName;
            this["Location"] = LimitEntryLength(e.LocationInformation.FullInfo);

            if (e.ExceptionObject != null)
            {
                this["Exception"] = LimitEntryLength(e.ExceptionObject.ToString());
            }

            foreach (DictionaryEntry entry in e.Properties)
            {
                var key = entry.Key.ToString()
                    .Replace(":", "_")
                    .Replace("@", "_")
                    .Replace(".", "_");

                // Ensure Message is < 32KB
                var entryValue = entry.Value;
                if (entryValue is string value )
                {
                    entryValue = LimitEntryLength(value);
                }

                this[key] = entryValue;
            }

            Timestamp = e.TimeStamp;
            PartitionKey = e.MakePartitionKey(partitionKeyType);
            RowKey = e.MakeRowKey();
        }

        public AzureDynamicLoggingEventEntity(LoggingEvent e, PartitionKeyTypeEnum partitionKeyType, string message, int sequenceNumber) : this(e, partitionKeyType)
        {
            this["Message"] = LimitEntryLength(message);
            this["SequenceNumber"] = sequenceNumber;
        }

        private string LimitEntryLength(string entry)
        {
            if (entry.Length > 30 * 1024)
            {
                return entry.Substring(0, 30 * 1024) + " [TRUNCATED]";
            }

            return entry;
        }
    }
}
