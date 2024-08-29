using System;
using System.IO;

namespace rsIMPHashHandler
{
    public class Logger
    {
        private static Logger instance;
        private static readonly object lockObj = new object();
        private static StreamWriter logFile;
        private const bool EXTENDED_LOGGING = false;

        private Logger() { }

        public static Logger Instance
        {
            get
            {
                lock (lockObj)
                {
                    if (instance == null)
                    {
                        instance = new Logger();
                    }
                    return instance;
                }
            }
        }

        public void Init(string logPath)
        {
            logFile = new StreamWriter(logPath, append: true);
            LogInfo("Start");
        }

        public void LogInfo(string message, bool isImportant = false)
        {
            logFile.WriteLine($"{DateTime.Now} INFO  {message}");
            logFile.Flush();
            if (isImportant)
            {
                var originalForegroundColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"{DateTime.Now} INFO  {message}");
                Console.ForegroundColor = originalForegroundColor;
            }
            else
            {
                Console.WriteLine($"{DateTime.Now} INFO  {message}");
            }
        }

        public void LogInfoEx(string message)
        {
            if (EXTENDED_LOGGING)
            {
                logFile.WriteLine($"{DateTime.Now} INFO  {message}");
                logFile.Flush();
            }
            Console.WriteLine($"{DateTime.Now} INFO  {message}");
        }

        public void LogWarn(string message)
        {
            logFile.WriteLine($"{DateTime.Now} WARN  {message}");
            logFile.Flush();

            var originalForegroundColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{DateTime.Now} WARN  {message}");
            Console.ForegroundColor = originalForegroundColor;
        }

        public void LogError(Exception ex, string message)
        {
            logFile.WriteLine($"{DateTime.Now} ERROR {message}: {ex.Message}");
            logFile.Flush();

            var originalForegroundColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.Now} ERROR {message}: {ex.Message}");
            Console.ForegroundColor = originalForegroundColor;
        }

        public void Close()
        {
            LogInfo("End");
            logFile.Close();
            logFile.Dispose();
        }
    }

}