using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Ports;
using System.Text;
using System.Threading;
using static QueryLog.Patch_OnExecutionCompleted;

namespace QueryLog
{
    public class Logger
    {
        private readonly int hashcode;
        private FileStream tempFile;
        private StreamWriter tempFileWriter;

        private object locker = new object();

        private SqliteConnection sqlite;

        private long currentPK = 0;
        private Stopwatch timer;

        public Logger(int hashcode)
        {
            this.hashcode = hashcode;

            var userDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            sqlite = new SqliteConnection();
            var sb = new StringBuilder();
            SqliteConnectionStringBuilder.AppendKeyValuePair(sb, "DataSource", Path.Combine(userDir, "ssms-querylogger.sqlite"));
            sqlite.ConnectionString = sb.ToString();
            sqlite.Open();

            EnsureSqlite();

        }

        private void EnsureSqlite()
        {
            var cmd = sqlite.CreateCommand();
            cmd.CommandText = @"
                pragma journal_mode=WAL;
                pragma foreign_keys = ON;

                create table if not exists query (
                    query_id integer primary key autoincrement,
                    date text not null,
                    server text not null,
                    db text not null,
                    connection_string text not null,
                    query_text text not null,
                    status int,
                    duration real,
                    rows_affected int
                ) strict;

                create table if not exists message (
                    message_id integer primary key autoincrement,
                    query_id integer,
                    date text not null,
                    message text,
                    detailed_message text,
                    foreign key(query_id) references query(query_id)
                ) strict;

                create table if not exists errmessage (
                    errmessage_id integer primary key autoincrement,
                    query_id integer,
                    date text not null,
                    detailed_message text,
                    description_message text,
                    line integer not null,
                    foreign key(query_id) references query(query_id)
                ) strict;
                ";
            cmd.ExecuteNonQuery();
        }

        public void OnStart(string server, string db, string connectionString, string query)
        {
            timer = Stopwatch.StartNew();
            var cmd = sqlite.CreateCommand();
            cmd.CommandText = @"
            insert into query (date, server, db, connection_string, query_text) values (@date, @server, @db, @connectionString, @query)
            returning query_id
            ";
            var datetimeoffseet = DateTimeOffset.Now;
            cmd.Parameters.Add(new SqliteParameter("@date", SqliteType.Text) { Value = datetimeoffseet.ToString("O") });
            cmd.Parameters.Add(new SqliteParameter("@server", SqliteType.Text) { Value = server });
            cmd.Parameters.Add(new SqliteParameter("@db", SqliteType.Text) { Value = db });
            cmd.Parameters.Add(new SqliteParameter("@connectionString", SqliteType.Text) { Value = connectionString });
            cmd.Parameters.Add(new SqliteParameter("@query", SqliteType.Text) { Value = query });
            currentPK = (long)cmd.ExecuteScalar();
            /*
            var sb = new StringBuilder();
            var time = DateTimeOffset.Now.ToString();
            sb.AppendLine($"{time} {server} {db}");
            sb.AppendLine(query.Trim());
            sb.AppendLine();

            lock (locker)
            {
                tempFile = File.Open($"currently-executing-{hashcode}.txt", FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
                tempFileWriter = new StreamWriter(tempFile, new UTF8Encoding(false), 1024, true);

                tempFileWriter.Write(sb.ToString());
            }
            */
        }

        public void OnEnd(ScriptExecutionResult result)
        {
            timer.Stop();
            var cmd = sqlite.CreateCommand();
            cmd.CommandText = @"
            update query set
                status = @status,
                duration = @duration
            where query_id = @id
            ";
            cmd.Parameters.Add(new SqliteParameter("@id", SqliteType.Integer) { Value = currentPK });
            if (result.HasFlag(ScriptExecutionResult.Failure)) result = ScriptExecutionResult.Failure;
            else if (result.HasFlag(ScriptExecutionResult.Cancel)) result = ScriptExecutionResult.Cancel;
            cmd.Parameters.Add(new SqliteParameter("@status", SqliteType.Integer) { Value = (int)result });
            cmd.Parameters.Add(new SqliteParameter("@duration", SqliteType.Real) { Value = timer.Elapsed.TotalSeconds });
            cmd.ExecuteNonQuery();
            /*
            var sb = new StringBuilder();
            var time = DateTimeOffset.Now.ToString();
            sb.AppendLine($"{time} result: {result}");

            lock (locker)
            {
                tempFileWriter.Write(sb.ToString());
                tempFileWriter.Dispose();
                tempFileWriter = null;

                tempFile.Position = 0;
                AppendFileToGlobal();
                tempFile.Dispose();
                tempFile = null;
            }
            */
        }

        public void OnMessage(string message, string detailedMessage)
        {
            var cmd = sqlite.CreateCommand();
            cmd.CommandText = @"
            insert into message (query_id, date, message, detailed_message) values (@id, @date, @message, @detailedMessage)
            ";
            cmd.Parameters.Add(new SqliteParameter("@id", SqliteType.Integer) { Value = currentPK });
            cmd.Parameters.Add(new SqliteParameter("@date", SqliteType.Text) { Value = DateTimeOffset.Now.ToString("O") });
            cmd.Parameters.Add(new SqliteParameter("@message", SqliteType.Text) { Value = message.Trim() });
            cmd.Parameters.Add(new SqliteParameter("@detailedMessage", SqliteType.Text) { Value = detailedMessage.Trim() });
            cmd.ExecuteNonQuery();
            /*
            var sb = new StringBuilder();
            var time = DateTimeOffset.Now.ToString();
            sb.AppendLine($"{time} {message} {detailedMessage}");

            lock (locker)
            {
                tempFileWriter.Write(sb.ToString());
            }
            */
        }

        public void OnErrorMessage(string detailedMessage, string descriptionMessage, int line)
        {
            var cmd = sqlite.CreateCommand();
            cmd.CommandText = @"
            insert into errmessage (query_id, date, detailed_message, description_message, line) values (@id, @date, @detailedMessage, @descriptionMessage, @line)
            ";
            cmd.Parameters.Add(new SqliteParameter("@id", SqliteType.Integer) { Value = currentPK });
            cmd.Parameters.Add(new SqliteParameter("@date", SqliteType.Text) { Value = DateTimeOffset.Now.ToString("O") });
            cmd.Parameters.Add(new SqliteParameter("@detailedMessage", SqliteType.Text) { Value = detailedMessage.Trim() });
            cmd.Parameters.Add(new SqliteParameter("@descriptionMessage", SqliteType.Text) { Value = descriptionMessage.Trim() });
            cmd.Parameters.Add(new SqliteParameter("@line", SqliteType.Integer) { Value = line });
            cmd.ExecuteNonQuery();
            /*
            var sb = new StringBuilder();
            var time = DateTimeOffset.Now.ToString();
            sb.AppendLine($"{time} {detailedMessage} {descriptionMessage} {line}");

            lock (locker)
            {
                tempFileWriter.Write(sb.ToString());
            }
            */
        }

        private void AppendFileToGlobal()
        {
            var mutex = new Mutex(false, "ssms-querylogger");
            mutex.WaitOne();

            using (var globalfile = File.Open("ssms-querylog.txt", FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                tempFile.CopyTo(globalfile);
            }

            mutex.ReleaseMutex();
        }
    }
}
