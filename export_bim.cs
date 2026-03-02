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
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            using var doc = System.Text.Json.JsonDocument.Parse(requestBody);

            string workspaceId = doc.RootElement.GetProperty("workspaceId").GetString() ?? "";
            string datasetName = doc.RootElement.GetProperty("datasetName").GetString() ?? "";

            if (string.IsNullOrWhiteSpace(workspaceId) || string.IsNullOrWhiteSpace(datasetName))
            {
                var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                await bad.WriteAsJsonAsync(new { error = "workspaceId and datasetName are required" });
                return bad;
            }

            // These come from Azure Function App Configuration (Environment Variables)
            string tenantId = Environment.GetEnvironmentVariable("TENANT_ID") ?? "";
            string clientId = Environment.GetEnvironmentVariable("CLIENT_ID") ?? "";
            string clientSecret = Environment.GetEnvironmentVariable("CLIENT_SECRET") ?? "";

            if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
            {
                var bad = req.CreateResponse(HttpStatusCode.BadRequest);
                await bad.WriteAsJsonAsync(new { error = "Missing TENANT_ID / CLIENT_ID / CLIENT_SECRET in Function App settings" });
                return bad;
            }

            // Get token
            var app = ConfidentialClientApplicationBuilder.Create(clientId)
                .WithClientSecret(clientSecret)
                .WithAuthority($"https://login.microsoftonline.com/{tenantId}")
                .Build();

            var token = await app.AcquireTokenForClient(
                new[] { "https://analysis.windows.net/powerbi/api/.default" })
                .ExecuteAsync();

            // XMLA endpoint for the workspace
            string xmlaEndpoint = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceId}";

            // Connect to XMLA using access token
            var server = new Server();
            server.Connect($"DataSource={xmlaEndpoint};Password={token.AccessToken};");

            // Find dataset database by name
            var db = server.Databases.Find(datasetName);
            if (db == null)
            {
                var notFound = req.CreateResponse(HttpStatusCode.NotFound);
                await notFound.WriteAsJsonAsync(new { error = $"Dataset '{datasetName}' not found in XMLA workspace." });
                return notFound;
            }

            // Export TMSL (this is the “model.bim style” JSON)
            string tmslJson = db.ScriptCreateOrReplace();

            // Return base64 so n8n can save it as a file
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
