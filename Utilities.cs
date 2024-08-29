using herdProtectLibrary.Chimera;
using herdProtectLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rsIMPHashHandler
{
    internal class Utilities
    {
        public static void IgnoreIMPHash(ImphashCounter imphash, string notes)
        {
            herdProtectLibrary.Signatures.SignaturesIgnore signaturesIgnore = new herdProtectLibrary.Signatures.SignaturesIgnore();
            signaturesIgnore.Type = herdProtectLibrary.Signatures.SignaturesIgnore.Types.IMPHash;
            signaturesIgnore.Value = imphash.imphash;
            signaturesIgnore.Source = "automation";
            signaturesIgnore.Notes = notes; 
            herdProtectLibrary.Signatures.SignaturesIgnores.save(ref signaturesIgnore);
        }
        public static bool IsSignedVT(Resource resource)
        {
            try
            {
                var resourceScansVT = herdProtectLibrary.ScannerVirusTotal.getFromAPIV3(resource.SHA1, out Scanner.Results result, out string message, "", out string json);
                if (result == herdProtectLibrary.Scanner.Results.Success && json.Length > 100)
                {
                    if (json.Contains("signature_info"))
                    {
                        herdProtectLibrary.Utilities.JSONUtils.JSONNode oJSONNode = herdProtectLibrary.Utilities.JSONUtils.JSON.Parse(json);
                        if (oJSONNode != null && oJSONNode["data"] != null && oJSONNode["data"]["attributes"] != null)
                        {
                            if (oJSONNode["data"]["attributes"]["signature_info"] != null)
                            {
                                if (oJSONNode["data"]["attributes"]["signature_info"]["verified"] != null)
                                {
                                    // if the value is "Signed" update the resource in herdprotect
                                    if (oJSONNode["data"]["attributes"]["signature_info"]["verified"].Value == "Signed")
                                    {
                                        Logger.Instance.LogInfoEx("ResourceID: " + resource.ResourceID +
                                                    " SignerName: " + resource.SignerName +
                                                    " SignerVerification: " + resource.SignerVerification +
                                                    " VT Value: " + oJSONNode["data"]["attributes"]["signature_info"]["verified"]);
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Instance.LogError(ex, "An error occurred while fixing signautes");
            }

            return false;
        }
        public static void ReportHashStats(bool isSuccess)
        {
            try
            {
                string status = isSuccess ? "Success" : "Failed";
                herdProtectLibrary.Hashstats.HashSet("STATS_Operations", "IMPHashCounterImport", DateTime.UtcNow.Ticks.ToString());
                herdProtectLibrary.Hashstats.HashSet("STATS_Operations", "IMPHashCounterImport_LastStatus", status);
            }
            catch { }
        }
    }
}
