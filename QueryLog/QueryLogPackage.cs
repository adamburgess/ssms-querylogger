using HarmonyLib;
using Microsoft.SqlServer.Management.QueryExecution;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static QueryLog.Patch_OnExecutionCompleted;
using Task = System.Threading.Tasks.Task;

namespace QueryLog
{
    [PackageRegistration(UseManagedResourcesOnly = true, AllowsBackgroundLoading = true)]
    [ProvideAutoLoad(UIContextGuids80.NoSolution, PackageAutoLoadFlags.BackgroundLoad)]
    [Guid(PackageGuidString)]
    public sealed class QueryLogPackage : AsyncPackage
    {
        public const string PackageGuidString = "b139db0f-da7c-4bc8-81c7-43cec51ff6bf";

        protected override Task InitializeAsync(CancellationToken cancellationToken, IProgress<ServiceProgressData> progress)
        {
            //await this.JoinableTaskFactory.SwitchToMainThreadAsync(cancellationToken);
            try
            {
                var harmony = new Harmony("patch.querylog");
                harmony.PatchAll();
            }
            catch (Exception e)
            {
                Debugger.Break();
            }

            return Task.CompletedTask;
        }

        private static ConditionalWeakTable<object, Logger> loggers = new ConditionalWeakTable<object, Logger>();
        private static Logger GetLoggerForBatch(object batch)
        {
            if (!loggers.TryGetValue(batch, out var logger))
            {
                logger = new Logger(batch.GetHashCode());
                loggers.Add(batch, logger);

            }
            return logger;
        }

        public static void OnStart(object batch, string server, string db, string connectionString, string query)
        {
            var logger = GetLoggerForBatch(batch);
            logger.OnStart(server, db, connectionString, query);
            Debug.WriteLine($"{batch.GetHashCode()} START {server} {db} {query}");
        }

        public static void OnEnd(object batch, ScriptExecutionResult result)
        {
            var logger = GetLoggerForBatch(batch);
            logger.OnEnd(result);
            Debug.WriteLine($"{batch.GetHashCode()} END {result}");
        }

        public static void OnMessage(object batch, string message, string detailedMessage)
        {
            var logger = GetLoggerForBatch(batch);
            logger.OnMessage(message, detailedMessage);
            Debug.WriteLine($"{batch.GetHashCode()} MSG {message.Trim()} {detailedMessage.Trim()}");
        }

        public static void OnErrorMessage(object batch, string detailedMessage, string descriptionMessage, int line)
        {
            var logger = GetLoggerForBatch(batch);
            logger.OnErrorMessage(detailedMessage, descriptionMessage, line);
            Debug.WriteLine($"{batch.GetHashCode()} ERR {detailedMessage.Trim()} {descriptionMessage.Trim()} {line}");
        }

    }

    class Accessor
    {
        public static Type QESQLBatch = AccessTools.TypeByName("Microsoft.SqlServer.Management.QueryExecution.QESQLBatch");
        public static MethodInfo QESQLBatch_Text = AccessTools.PropertyGetter(QESQLBatch, "Text");
        public static MethodInfo SqlConnection_DataSource = AccessTools.PropertyGetter("Microsoft.Data.SqlClient.SqlConnection:DataSource");

        public static EventInfo QESQLBatch_Message = QESQLBatch.GetEvent("Message");
        public static EventInfo QESQLBatch_ErrorMessage = QESQLBatch.GetEvent("ErrorMessage");

        public static Type QESQLBatchMessageEventArgs = QESQLBatch_Message.EventHandlerType.GetMethod("Invoke").GetParameters()[1].ParameterType;
        public static MethodInfo QESQLBatchMessageEventArgs_Message = AccessTools.PropertyGetter(QESQLBatchMessageEventArgs, "Message");
        public static MethodInfo QESQLBatchMessageEventArgs_DetailedMessage = AccessTools.PropertyGetter(QESQLBatchMessageEventArgs, "DetailedMessage");

        public static Type QESQLBatchErrorMessageEventArgs = QESQLBatch_ErrorMessage.EventHandlerType.GetMethod("Invoke").GetParameters()[1].ParameterType;
        public static MethodInfo QESQLBatchErrorMessageEventArgs_DetailedMessage = AccessTools.PropertyGetter(QESQLBatchErrorMessageEventArgs, "DetailedMessage");
        public static MethodInfo QESQLBatchErrorMessageEventArgs_DescriptionMessage = AccessTools.PropertyGetter(QESQLBatchErrorMessageEventArgs, "DescriptionMessage");
        public static MethodInfo QESQLBatchErrorMessageEventArgs_Line = AccessTools.PropertyGetter(QESQLBatchErrorMessageEventArgs, "Line");
    }

    [HarmonyPatch("Microsoft.SqlServer.Management.QueryExecution.QESQLExec", "StartExecuting")]
    public class Patch_StartExecuting
    {
        public static bool Prefix(object __instance, ref IDbConnection ___m_conn, ref object ___m_curBatch, ref ITextSpan ___textSpan)
        {
            var server = (string)Accessor.SqlConnection_DataSource.Invoke(___m_conn, null);
            var db = ___m_conn.Database;
            var connectionString = ___m_conn.ConnectionString;

            //var sql = (string)Accessor.QESQLBatch_Text.Invoke(___m_curBatch, null);
            var sql = ___textSpan.Text;

            QueryLogPackage.OnStart(___m_curBatch, server, db, connectionString, sql);

            return true;
        }
    }

    [HarmonyPatch("Microsoft.SqlServer.Management.QueryExecution.QESQLExec", "OnExecutionCompleted")]
    public class Patch_OnExecutionCompleted
    {
        [Flags]
        public enum ScriptExecutionResult
        {
            // Token: 0x0400000A RID: 10
            Success = 1,
            // Token: 0x0400000B RID: 11
            Failure = 2,
            // Token: 0x0400000C RID: 12
            Cancel = 4,
            // Token: 0x0400000D RID: 13
            Timeout = 8,
            // Token: 0x0400000E RID: 14
            Halted = 16,
            // Token: 0x0400000F RID: 15
            Mask = 31
        }
        public static bool Prefix(object __instance, ref object ___m_curBatch, int execResult)
        {
            var resultEnum = (ScriptExecutionResult)execResult;

            QueryLogPackage.OnEnd(___m_curBatch, resultEnum);
            return true;
        }
    }


    [HarmonyPatch("Microsoft.SqlServer.Management.QueryExecution.QESQLExec", "HookupBatchWithConsumer")]
    public class Patch_HookupBatchWithConsumer
    {

        public static void OnMessage(object sender, object messageArgs)
        {
            var message = (string)Accessor.QESQLBatchMessageEventArgs_Message.Invoke(messageArgs, null);
            var detailedMessage = (string)Accessor.QESQLBatchMessageEventArgs_DetailedMessage.Invoke(messageArgs, null);

            QueryLogPackage.OnMessage(sender, message, detailedMessage);
        }
        public static void OnErrorMessage(object sender, object messageArgs)
        {
            var detailedMessage = (string)Accessor.QESQLBatchErrorMessageEventArgs_DetailedMessage.Invoke(messageArgs, null);
            var descriptionMessage = (string)Accessor.QESQLBatchErrorMessageEventArgs_DescriptionMessage.Invoke(messageArgs, null);
            var line = (int)Accessor.QESQLBatchErrorMessageEventArgs_Line.Invoke(messageArgs, null);

            QueryLogPackage.OnErrorMessage(sender, detailedMessage, descriptionMessage, line);
        }

        static Delegate OnMessageDelegate = Delegate.CreateDelegate(Accessor.QESQLBatch_Message.EventHandlerType, typeof(Patch_HookupBatchWithConsumer).GetMethod(nameof(OnMessage)));
        static Delegate OnErrorMessageDelegate = Delegate.CreateDelegate(Accessor.QESQLBatch_ErrorMessage.EventHandlerType, typeof(Patch_HookupBatchWithConsumer).GetMethod(nameof(OnErrorMessage)));

        public static bool Prefix(object __instance, object batch, object batchConsumer, bool bHookUp)
        {
            if (bHookUp)
            {
                Accessor.QESQLBatch_Message.AddEventHandler(batch, OnMessageDelegate);
                Accessor.QESQLBatch_ErrorMessage.AddEventHandler(batch, OnErrorMessageDelegate);
            }
            else
            {
                Accessor.QESQLBatch_Message.RemoveEventHandler(batch, OnMessageDelegate);
                Accessor.QESQLBatch_ErrorMessage.RemoveEventHandler(batch, OnErrorMessageDelegate);
            }
            return true;
        }
    }
}
/*

Every Window gets its own DisplaySQLResultsControl.
This has a QESQLExec, which has a QESQLBatch.

There are a couple places we can get the database:

1. QESQLBatch.Execute
2. QESQLBatch.DoBatchExecution (called by Execute OR QEOLESQLExec.DoScriptExecution)
3. QEOLESQLExec.DoBatchExecution (get m_currentCon, calls batch.Execute)
4. QESQLExec.Execute (sets m_conn)


----
Hook QESQLExec.OnStartBatchExecution -- Grab the batch (contains the Text) and this.CurrentCon
Hook QESQLExec.OnExecutionCompleted -- This is always called, even when cancelled.
Hook QESQLExec.HookupBatchWithConsumer
    Add (or remove) event handlers for Message and ErrorMessage

-----


Output

3 files

1. sqlite db, which can be read/write by many windows at once
2. single output file. locks are used to write the finished messages to this file
3. currently executing command - temp file.

process:

1. execute "select a GO select 1"
2. new command inserted into sqlite
3. new temp file created, command written in
4. error occurs in 'select a'. message written into sqlite. message written into temp file.
5. command finishes. sqlite command updated. outputs written into temp file
6. single output file taken lock
7. append temp file into output
8. lock released


For every command executed, print:

1. date/time
2. server, database
3. number of rows in result
4. any messages (e.g. errors)


Steps:

1. Hook method call QESQLExec.Execute
    Parametrs: Text span to execute, connection
2. It has an event callback BatchExectionCompleted. Add onto that to see the result.
3. m_curBatch has events ErrorMessage and Message. Add onto that to see messages for this query.

*/
