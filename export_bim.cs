using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.Identity.Client;

public class export_bim
{
    private readonly ILogger _logger;

    public export_bim(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<export_bim>();
    }

    [Function("export_bim")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        try
        {
            // ---- Read request ----
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            using var doc = System.Text.Json.JsonDocument.Parse(requestBody);

            string workspaceId = doc.RootElement.TryGetProperty("workspaceId", out var wsProp)
                ? (wsProp.GetString() ?? "")
                : "";

            string datasetName = doc.RootElement.TryGetProperty("datasetName", out var dsProp)
                ? (dsProp.GetString() ?? "")
                : "";

            if (string.IsNullOrWhiteSpace(workspaceId) || string.IsNullOrWhiteSpace(datasetName))
            {
                var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                await bad.WriteAsJsonAsync(new { error = "workspaceId and datasetName are required" });
                return bad;
            }

            // ---- Read env vars ----
            string tenantId = Environment.GetEnvironmentVariable("TENANT_ID") ?? "";
            string clientId = Environment.GetEnvironmentVariable("CLIENT_ID") ?? "";
            string clientSecret = Environment.GetEnvironmentVariable("CLIENT_SECRET") ?? "";

            if (string.IsNullOrWhiteSpace(tenantId) ||
                string.IsNullOrWhiteSpace(clientId) ||
                string.IsNullOrWhiteSpace(clientSecret))
            {
                var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                await bad.WriteAsJsonAsync(new
                {
                    error = "Missing TENANT_ID / CLIENT_ID / CLIENT_SECRET in Function App settings"
                });
                return bad;
            }

            // ---- Acquire token ----
            var app = ConfidentialClientApplicationBuilder.Create(clientId)
                .WithClientSecret(clientSecret)
                .WithAuthority($"https://login.microsoftonline.com/{tenantId}")
                .Build();

            var token = await app.AcquireTokenForClient(
                    new[] { "https://analysis.windows.net/powerbi/api/.default" })
                .ExecuteAsync();

            // ---- Connect XMLA ----
            // NOTE: XMLA usually needs WORKSPACE NAME (not GUID).
            string xmlaEndpoint = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceId}";

            var server = new Server();
            server.Connect($"DataSource={xmlaEndpoint};Password={token.AccessToken};");

            // ---- Find the dataset/model in XMLA (NO LINQ) ----
            Database? db = null;

            // 1) Exact match (case-insensitive)
            foreach (Database d in server.Databases)
            {
                if (!string.IsNullOrWhiteSpace(d.Name) &&
                    string.Equals(d.Name, datasetName, StringComparison.OrdinalIgnoreCase))
                {
                    db = d;
                    break;
                }
            }

            // 2) Contains match (case-insensitive)
            if (db == null)
            {
                foreach (Database d in server.Databases)
                {
                    if (!string.IsNullOrWhiteSpace(d.Name) &&
                        d.Name.IndexOf(datasetName, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        db = d;
                        break;
                    }
                }
            }

            // 3) Still not found → return all model names
            if (db == null)
            {
                var names = new string[server.Databases.Count];
                int i = 0;
                foreach (Database d in server.Databases)
                {
                    names[i++] = d.Name ?? "";
                }

                var notFound = req.CreateResponse(HttpStatusCode.NotFound);
                await notFound.WriteAsJsonAsync(new
                {
                    error = $"Dataset '{datasetName}' not found in XMLA workspace.",
                    hint = "Use one of the availableModels values as datasetName (these are the real XMLA model names).",
                    availableModels = names
                });
                return notFound;
            }

            // ---- Export model as JSON ----
            string tmslJson;
            try
            {
                var options = new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                };

                // NOTE: This is a JSON representation of the Tabular model object.
                // If serialization ever fails, we return a clear error message.
                tmslJson = System.Text.Json.JsonSerializer.Serialize(db.Model, options);
            }
            catch (Exception serEx)
            {
                _logger.LogError(serEx, "Model serialization failed");
                var errSer = req.CreateResponse(HttpStatusCode.InternalServerError);
                await errSer.WriteAsJsonAsync(new
                {
                    error = "Failed to serialize model to JSON with System.Text.Json.",
                    details = serEx.Message
                });
                return errSer;
            }

            // ---- Return base64 ----
            string base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(tmslJson));

            var ok = req.CreateResponse(HttpStatusCode.OK);
            await ok.WriteAsJsonAsync(new
            {
                fileName = $"{db.Name}.bim",
                resolvedModelName = db.Name,
                contentBase64 = base64
            });

            return ok;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "export_bim failed");

            var err = req.CreateResponse(HttpStatusCode.InternalServerError);
            await err.WriteAsJsonAsync(new { error = ex.Message, stack = ex.StackTrace });
            return err;
        }
    }
}
