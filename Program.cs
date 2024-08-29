using Amazon.Runtime.SharedInterfaces;
using Amazon.Util.Internal;
using herdProtectLibrary;
using herdProtectLibrary.Chimera;
using herdProtectLibrary.Signatures;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Threading;
using System.Threading.Tasks;
using static rsIMPHashHandler.Program;

namespace rsIMPHashHandler
{
    internal class Program
    {
        public enum ExecutionStatus
        {
            SUCCESS = 0,
            FAILURE = 1,
            NOTHING_TO_DO = 2
        }

        static void Main(string[] args)
        {
            // log the beginnig of the run 
            Logger.Instance.Init("debug.dat");
            ExecutionStatus executionStatus = ExecutionStatus.SUCCESS;

            long? proccessedResouresID = null;

            executionStatus = LoadProcessedResourceID(ref proccessedResouresID);
            if (executionStatus != ExecutionStatus.NOTHING_TO_DO)
            {
                executionStatus = CalculateIMPHashesCounters(proccessedResouresID);
            }
            executionStatus = HandleIgnoreList(executionStatus);

            Logger.Instance.Close();
            Utilities.ReportHashStats(executionStatus != ExecutionStatus.FAILURE);
        }
        private static ExecutionStatus LoadProcessedResourceID(ref long? proccessedResouresID)
        {
            try
            {
                //read proceesedResourceID from processeedResouresID.dat
                if (File.Exists("processeedResouresID.dat"))
                {
                    proccessedResouresID = Convert.ToInt64(File.ReadAllText("processeedResouresID.dat"));
                    Logger.Instance.LogInfo($"processeedResouresID: {proccessedResouresID}");
                }
                else
                {
                    Logger.Instance.LogError(new Exception("Failed to load proceesedResourceID.dat"), "Error occured");
                    return ExecutionStatus.FAILURE;
                }


                Resource resource = Resources.load(proccessedResouresID.Value, true);
                if (resource == null || resource.CreateDate.Date >= DateTime.Now.Date)
                {
                    Logger.Instance.LogWarn($"All up-to-date. Run again tomorrow to get the IMPHash counters of yesterday. Exiting.");
                    return ExecutionStatus.NOTHING_TO_DO;
                }
            }
            catch (Exception ex)
            {
                Logger.Instance.LogError(ex, "Unexpected error occurred");
                return ExecutionStatus.FAILURE;
            }
            return ExecutionStatus.SUCCESS;
        }
        private static ExecutionStatus CalculateIMPHashesCounters(long? proccessedResouresID)
        {
            try
            {
                const int resourceIDInterval = 100000; // Interval of 100,000 ResourceIDs.
                long? lastResourceID = proccessedResouresID;
                long? nextResourceID = null;
                long? currentResourceID = herdProtectLibrary.Resources.getResourceIDByHoursAgo(0);

                if (!lastResourceID.HasValue || !currentResourceID.HasValue || proccessedResouresID.Value > currentResourceID.Value)
                {
                    Logger.Instance.LogWarn("Failed to load the last or current ResourceIDs");
                    throw new Exception("Failed to load the last or current ResourceIDs");
                }

                Logger.Instance.LogInfo($"Last ResourceID: {lastResourceID}");
                Logger.Instance.LogInfo($"Current ResourceID: {currentResourceID}");

                List<ImphashCounter> accumulatedImphashCounters = new List<ImphashCounter>();
                DateTime? currentCreateDate = null;

                while (true)
                {
                    nextResourceID = lastResourceID + resourceIDInterval;

                    if (nextResourceID > currentResourceID)
                    {
                        Logger.Instance.LogInfo($"Reached the current ResourceID: {currentResourceID}. Processing remaining accumulated data.");
                        nextResourceID = currentResourceID;
                    }
                    Logger.Instance.LogInfoEx($"Last ResourceID: {lastResourceID} Next ResourceID: {nextResourceID}");


                    // Consider the "CreateDate" of the last ResourceID in the current batch to control accumulation.
                    DateTime createDate = Resources.load(lastResourceID.Value).CreateDate.Date;


                    if (nextResourceID.HasValue)
                    {
                        string query = $"SELECT r.\"IMPHash\", COUNT(1) AS count FROM dbo.\"Resources\" r LEFT JOIN dbo.\"SignaturesIgnores\" sig ON r.\"IMPHash\" = sig.\"Value\" WHERE r.\"ResourceID\" < {nextResourceID} AND r.\"ResourceID\" >= {lastResourceID} AND r.\"IMPHash\" IS NOT NULL AND r.\"IMPHash\" != '' AND sig.\"Value\" IS NULL GROUP BY r.\"IMPHash\"";
                        using (NpgsqlCommand command = new NpgsqlCommand(query))
                        {
                            // Measure time spent on the next line
                            DateTime start = DateTime.Now;
                            DataSet dataset = Reason.DatabaseAccess.PopulateDataSet(Settings.DBConnectionString_herdProtect_ADHOC, command, 60 * 10);
                            DateTime end = DateTime.Now;
                            Logger.Instance.LogInfoEx($"Query took: {end - start}");

                            if (dataset == null || dataset.Tables.Count == 0 || dataset.Tables[0].Rows.Count == 0)
                            {
                                Logger.Instance.LogWarn($"No data found for the interval of ResourceIDs from {lastResourceID} to {nextResourceID}");
                                break; // Exit the loop if no data found
                            }

                            foreach (DataRow row in dataset.Tables[0].Rows)
                            {
                                string imphash = Convert.ToString(row["IMPHash"]);
                                long counter = Convert.ToInt64(row["count"]);

                                // Find if the imphash is already accumulated
                                var existingCounter = accumulatedImphashCounters.FirstOrDefault(c => c.imphash == imphash);

                                if (existingCounter != null)
                                {
                                    existingCounter.counter += counter;
                                }
                                else
                                {
                                    accumulatedImphashCounters.Add(new ImphashCounter
                                    {
                                        imphash = imphash,
                                        counter = counter,
                                        create_date = createDate
                                    });
                                }
                            }
                        }

                        // Save and reset accumulation if the current batch's date differs from the previous one.
                        if (currentCreateDate != null && currentCreateDate != createDate)
                        {
                            accumulatedImphashCounters = accumulatedImphashCounters.Where(c => c.counter > 1).ToList();
                            Logger.Instance.LogInfo($"Saving {accumulatedImphashCounters.Count} ImphashCounters to the database for {currentCreateDate} from ResourceID {lastResourceID} to ResourceID {nextResourceID}", true);
                            ImphashCounters.save(ref accumulatedImphashCounters); //will throw exception if failed to save

                            //save proceesedResourceID to processeedResouresID.dat
                            File.WriteAllText("processeedResouresID.dat", nextResourceID.ToString());

                            accumulatedImphashCounters.Clear();
                            proccessedResouresID = nextResourceID;
                        }

                        currentCreateDate = createDate;

                        // Preparing for the next iteration
                        lastResourceID = nextResourceID;
                    }
                    else
                    {
                        Logger.Instance.LogWarn($"No previous resource ID found for interval of {resourceIDInterval} ResourceIDs ago.");
                        break; // Exit the loop if no previous resource ID is found
                    }
                }

                // Save remaining accumulated data
                if (accumulatedImphashCounters.Count > 0 && currentCreateDate != null)
                {
                    //Clear records with only 1 counter
                    accumulatedImphashCounters = accumulatedImphashCounters.Where(c => c.counter > 1).ToList();
                    Logger.Instance.LogInfo($"Saving {accumulatedImphashCounters.Count} ImphashCounters to the database for {currentCreateDate} from ResourceID {lastResourceID} to ResourceID {nextResourceID}", true);
                    ImphashCounters.save(ref accumulatedImphashCounters);

                    //save proceesedResourceID to processeedResouresID.dat
                    File.WriteAllText("processeedResouresID.dat", nextResourceID.ToString());

                }
            }
            catch (Exception ex)
            {
                Logger.Instance.LogError(ex, "An error occurred in the while sampling imphasher counters");
                return ExecutionStatus.FAILURE;
            }

            return ExecutionStatus.SUCCESS;
        }
        
        private static ExecutionStatus HandleIgnoreList(ExecutionStatus executionStatus)
        {
            try
            {
                List<ImphashCounter> imphashes = ImphashCounters.Get_by_create_date(DateTime.Now.Date.AddDays(-15), DateTime.Now.Date.AddDays(-10), 300000);
                Logger.Instance.LogInfo($"Found {imphashes.Count} IMPHashes to process", true);

                // Shuffle the list to get the chance to process all the imphashes in multiple runs
                imphashes = imphashes.OrderBy(a => Guid.NewGuid()).ToList();

                HashSet<string> imphashesSet = new HashSet<string>();
                int maxDegreeOfParallelism = 50;

                Task[] tasks = new Task[maxDegreeOfParallelism];
                for (int inx = 0; inx < tasks.Length; inx++)
                {
                    tasks[inx] = Task.CompletedTask;
                }

                int taskIndex = 0;
                foreach (ImphashCounter imphash in imphashes)
                {
                    // Make sure we process every hash only once
                    if (imphashesSet.Contains(imphash.imphash) ||
                        SignaturesIMPHashes.get_byIMPHash(imphash.imphash).Count > 0 ||
                        SignaturesIgnores.get_byTypeValue(SignaturesIgnore.Types.IMPHash, imphash.imphash, true).Count > 0)
                    {
                        continue;
                    }

                    imphashesSet.Add(imphash.imphash);

                    // Schedule the task and cyrcle the task array
                    int currentIndex = taskIndex;
                    tasks[currentIndex] = tasks[currentIndex].ContinueWith(t => EvaluateImphashToIgnore(currentIndex, imphash));
                    taskIndex = (taskIndex + 1) % maxDegreeOfParallelism;
                }

                // Wait for all tasks to complete
                Task.WhenAll(tasks).Wait();
            }
            catch (Exception ex)
            {
                Logger.Instance.LogError(ex, "An error occurred in the while updating the ignore list");
                executionStatus = ExecutionStatus.FAILURE;
            }

            return executionStatus;
        }
        private static void EvaluateImphashToIgnore(int inx, ImphashCounter imphash)
        {
            try
            {
                var resources = herdProtectLibrary.Resources.getADHOC("SELECT * FROM \"Resources\" WHERE \"IMPHash\"='" + imphash.imphash + "' ORDER BY \"ResourceID\" DESC LIMIT 1000");
                Logger.Instance.LogInfoEx("Processing #{" + inx + "} " + imphash.imphash + " with counter " + imphash.counter + " listed resources " + resources.Count);
                if (resources?.Count > 1)
                {
                    // Detections count
                    int detected = resources.Count(r => r.DeterminationPositive == 1);
                    int viruses = resources.Count(r => r.DeterminationName.StartsWith("Virus", StringComparison.OrdinalIgnoreCase));
                    int worms = resources.Count(r => r.DeterminationName.StartsWith("Worm", StringComparison.OrdinalIgnoreCase));

                    string bestdetecton = "";
                    if (detected > 0)
                    {
                        bestdetecton = resources
                            .Where(r => r.DeterminationName.Length > 0)
                            .GroupBy(o => o.DeterminationName)
                            .OrderByDescending(g => g.Count())
                            .First()
                            .Key;
                    }

                    // Check for ones were virus is the most
                    if (viruses > 0 && bestdetecton.StartsWith("Virus", StringComparison.OrdinalIgnoreCase))
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Virus");
                        Utilities.IgnoreIMPHash(imphash, "virus");
                        return;
                    }

                    // Check for ones were worm is the most
                    if (worms > 0 && bestdetecton.StartsWith("Worm", StringComparison.OrdinalIgnoreCase))
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Worm");
                        Utilities.IgnoreIMPHash(imphash, "worm");
                        return;
                    }


                    // Check if there are any safe files
                    if (resources.Exists(r => r.IsSafe == 1) || resources.Exists(r => r.isProbablySafe() == true) || resources.Exists(r => r.isWhitelisted() == true) || resources.Exists(r => r.FileWFPProtected == 1))
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Safe");
                        Utilities.IgnoreIMPHash(imphash, "safe");
                        return;
                    }

                    // Check if there are old files
                    if (resources.Exists(r => r.CreateDate < DateTime.Now.AddYears(-2)))
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Old");
                        Utilities.IgnoreIMPHash(imphash, "old");
                        return;
                    }

                    // Check if File is Installer
                    if (resources.Exists(r => r.FilePEInstallerType.Length > 0))
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Installer");
                        Utilities.IgnoreIMPHash(imphash, "installer");
                        return;
                    }

                    // Counters
                    var resources_counters = herdProtectLibrary.resources_counters.get(resources.Select(r => r.SHA1).ToList(), true);
                    if (resources_counters?.Count > 0 && resources_counters.Sum(r => r.counter) > 5000)
                    {
                        Logger.Instance.LogInfo("Remove " + imphash.imphash + " for Counter");
                        Utilities.IgnoreIMPHash(imphash, "count");
                        return;
                    }

                    // Get all resources that have SignerVerification != 0 and check if VT recognize them as signed and ignore them
                    var resourcsOfMS = resources.FindAll(
                        r => r.SignerVerification != 0 &&
                        r.SignerVerification != 256 /*SubjectCertExpired*/ &&
                        Resources.signersProbablyIgnore().Any(s => s.Equals(r.SignerName, StringComparison.OrdinalIgnoreCase)));
                    //reduce the the list to up to 10 resources
                    if (resourcsOfMS.Count > 10)
                    {
                        resourcsOfMS = resourcsOfMS.GetRange(0, 10);
                    }
                    foreach (var r in resourcsOfMS)
                    {
                        if (Utilities.IsSignedVT(r))
                        {
                            Logger.Instance.LogInfo("Remove " + imphash.imphash + " for SignerVerification");
                            Utilities.IgnoreIMPHash(imphash, "signfix");
                            continue;
                        }
                    }
                }
            }
            catch (Exception ex)
            { 
                Logger.Instance.LogError(ex, "An error occurred while evaluating imphash to ignore"); 
            }
        }

    }
}
