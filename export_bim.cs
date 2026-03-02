using System;
using System.IO;
using System.Linq;
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
            // NOTE: this path usually needs WORKSPACE NAME (not GUID).
            string xmlaEndpoint = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceId}";

            var server = new Server();
            server.Connect($"DataSource={xmlaEndpoint};Password={token.AccessToken};");

            // ---- Find the dataset/model in XMLA ----
            // 1) Exact match (case-insensitive)
            var db = server.Databases
                .FirstOrDefault(d => string.Equals(d.Name, datasetName, StringComparison.OrdinalIgnoreCase));

            // 2) Contains match (case-insensitive) - helps when names differ slightly
            if (db == null)
            {
                db = server.Databases
                    .FirstOrDefault(d => d.Name != null &&
                                         d.Name.IndexOf(datasetName, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            // 3) If still not found, return list of actual XMLA model names
            if (db == null)
            {
                var available = server.Databases
                    .Where(d => !string.IsNullOrWhiteSpace(d.Name))
                    .Select(d => d.Name)
                    .OrderBy(n => n)
                    .ToArray();

                var notFound = req.CreateResponse(HttpStatusCode.NotFound);
                await notFound.WriteAsJsonAsync(new
                {
                    error = $"Dataset '{datasetName}' not found in XMLA workspace.",
                    hint = "Use one of the availableModels values as datasetName (these are the real XMLA model names).",
                    availableModels = available
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
                contentBase64 = base64,
                resolvedModelName = db.Name
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
