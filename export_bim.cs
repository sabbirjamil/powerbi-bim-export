using System;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
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
            using var doc = JsonDocument.Parse(requestBody);

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
            // NOTE: workspaceId here is assumed to be the workspace NAME used by XMLA path
            // If you are passing workspace GUID, this might not work. If it fails, we'll map ID->Name.
            string xmlaEndpoint = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceId}";

            var server = new Server();
            server.Connect($"DataSource={xmlaEndpoint};Password={token.AccessToken};");

            var db = server.Databases.Find(datasetName);
            if (db == null)
            {
                var notFound = req.CreateResponse(HttpStatusCode.NotFound);
                await notFound.WriteAsJsonAsync(new
                {
                    error = $"Dataset '{datasetName}' not found in XMLA workspace."
                });
                return notFound;
            }

            // ---- Export model as JSON ----
            // Some Tabular objects may not serialize perfectly; we catch and return a clear error.
            string tmslJson;
            try
            {
                var options = new JsonSerializerOptions
                {
                    WriteIndented = true
                };

                tmslJson = JsonSerializer.Serialize(db.Model, options);
            }
            catch (Exception serEx)
            {
                _logger.LogError(serEx, "Model serialization failed");
                var errSer = req.CreateResponse(HttpStatusCode.InternalServerError);
                await errSer.WriteAsJsonAsync(new
                {
                    error = "Failed to serialize model to JSON. Library object may not be serializable with System.Text.Json.",
                    details = serEx.Message
                });
                return errSer;
            }

            // ---- Return base64 ----
            string base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(tmslJson));

            var ok = req.CreateResponse(HttpStatusCode.OK);
            await ok.WriteAsJsonAsync(new
            {
                fileName = $"{datasetName}.bim",
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
