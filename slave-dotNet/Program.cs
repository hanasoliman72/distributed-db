using System.Text.Json;
using MySqlConnector;

// ════════════════════════════════════════════════════════════════════════════
//  .NET Slave Node  –  port 8080 (or set env SLAVE_PORT)
//  Special feature: aggregations (COUNT, AVG, MAX, MIN) via /query/aggregate
// ════════════════════════════════════════════════════════════════════════════

const string MysqlUser     = "root";
const string MysqlPassword = "root";        // ← change to your password
const string MysqlHost     = "127.0.0.1";
const string MysqlPort     = "3306";
const string SlavePort     = "8080";

var connStr = $"Server={MysqlHost};Port={MysqlPort};User ID={MysqlUser};Password={MysqlPassword};AllowPublicKeyRetrieval=true;SslMode=None;";

// Verify connection on startup
try
{
    using var testConn = new MySqlConnection(connStr);
    testConn.Open();
    Console.WriteLine($"[slave-dotnet] Connected to MySQL at {MysqlHost}:{MysqlPort}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Cannot connect to MySQL: {ex.Message}");
    return;
}

var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
};

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort}");
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.TimestampFormat = "[HH:mm:ss] ");
var app = builder.Build();
var log = app.Logger;

// ── Storage helpers ───────────────────────────────────────────────────────

MySqlConnection Open()
{
    var c = new MySqlConnection(connStr);
    c.Open();
    return c;
}

async Task Exec(string sql, Dictionary<string, object?>? p = null)
{
    await using var conn = Open();
    await using var cmd  = new MySqlCommand(sql, conn);
    if (p != null) foreach (var (k, v) in p) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
}

async Task<List<Dictionary<string, object?>>> QueryRows(string sql, Dictionary<string, object?>? p = null)
{
    await using var conn   = Open();
    await using var cmd    = new MySqlCommand(sql, conn);
    if (p != null) foreach (var (k, v) in p) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await using var reader = await cmd.ExecuteReaderAsync();
    var rows = new List<Dictionary<string, object?>>();
    while (await reader.ReadAsync())
    {
        var row = new Dictionary<string, object?>();
        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        rows.Add(row);
    }
    return rows;
}

(string Clause, Dictionary<string, object?> Params) BuildWhere(Dictionary<string, object?> where)
{
    var clauses = new List<string>();
    var parms   = new Dictionary<string, object?>();
    foreach (var (k, v) in where) { clauses.Add($"`{k}` = @wh_{k}"); parms[$"@wh_{k}"] = v?.ToString(); }
    return (string.Join(" AND ", clauses), parms);
}

Dictionary<string, object?> GetDict(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? JsonSerializer.Deserialize<Dictionary<string, object?>>(el.GetRawText(), jsonOpts) ?? new()
        : new();

List<string> GetStringList(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? el.EnumerateArray().Select(x => x.GetString() ?? "").ToList()
        : new();

IResult Ok(object data) => Results.Json(new { success = true,  data }, jsonOpts);
IResult Fail(string msg) => Results.Json(new { success = false, error = msg }, jsonOpts, statusCode: 500);
IResult Bad(string msg)  => Results.Json(new { success = false, error = msg }, jsonOpts, statusCode: 400);

// ── Health ────────────────────────────────────────────────────────────────

app.MapGet("/health", () =>
    Results.Json(new { status = "ok", role = "slave-dotnet" }, jsonOpts));

// ════════════════════════════════════════════════════════════════════════════
//  DB REPLICATION  ← these were missing in the original C# slave
// ════════════════════════════════════════════════════════════════════════════

// POST /replicate/db/create
// Body: { "db": "mydb" }
app.MapPost("/replicate/db/create", async (HttpRequest req) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    if (string.IsNullOrWhiteSpace(db)) return Bad("'db' is required.");
    try
    {
        await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");
        log.LogInformation("[REPLICATE] CREATE_DB {Db}", db);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// POST /replicate/db/drop
// Body: { "db": "mydb" }
app.MapPost("/replicate/db/drop", async (HttpRequest req) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    if (string.IsNullOrWhiteSpace(db)) return Bad("'db' is required.");
    try
    {
        await Exec($"DROP DATABASE IF EXISTS `{db}`");
        log.LogInformation("[REPLICATE] DROP_DB {Db}", db);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ── Table Replication ─────────────────────────────────────────────────────

app.MapPost("/replicate/table/create", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var attrs = GetStringList(body, "attributes");
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table)) return Bad("'db' and 'table' required.");
    try
    {
        await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");
        var cols = new List<string> { "`id` INT AUTO_INCREMENT PRIMARY KEY" };
        foreach (var a in attrs)
            if (!a.Equals("id", StringComparison.OrdinalIgnoreCase))
                cols.Add($"`{a}` TEXT");
        await Exec($"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({string.Join(", ", cols)})");
        log.LogInformation("[REPLICATE] CREATE_TABLE {Db}.{Tbl}", db, table);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

app.MapPost("/replicate/table/drop", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    try
    {
        await Exec($"DROP TABLE IF EXISTS `{db}`.`{table}`");
        log.LogInformation("[REPLICATE] DROP_TABLE {Db}.{Tbl}", db, table);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ── Query Replication ─────────────────────────────────────────────────────

app.MapPost("/replicate/query/insert", async (HttpRequest req) =>
{
    var body   = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db     = body.GetProperty("db").GetString()    ?? "";
    var table  = body.GetProperty("table").GetString() ?? "";
    var record = GetDict(body, "record");
    if (record.Count == 0) return Bad("'record' cannot be empty.");
    try
    {
        var cols = new List<string>(); var phs = new List<string>(); var parms = new Dictionary<string, object?>();
        foreach (var (col, val) in record)
        { cols.Add($"`{col}`"); phs.Add($"@{col}"); parms[$"@{col}"] = val?.ToString(); }
        await Exec($"INSERT INTO `{db}`.`{table}` ({string.Join(", ", cols)}) VALUES ({string.Join(", ", phs)})", parms);
        log.LogInformation("[REPLICATE] INSERT → {Db}.{Tbl}", db, table);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

app.MapPost("/replicate/query/update", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    var set   = GetDict(body, "set");
    if (set.Count == 0) return Bad("'set' cannot be empty.");
    try
    {
        var setClauses = set.Keys.Select(k => $"`{k}` = @set_{k}").ToList();
        var parms      = new Dictionary<string, object?>();
        foreach (var (k, v) in set) parms[$"@set_{k}"] = v?.ToString();
        var sql = $"UPDATE `{db}`.`{table}` SET {string.Join(", ", setClauses)}";
        if (where.Count > 0) { var (clause, wp) = BuildWhere(where); sql += " WHERE " + clause; foreach (var (k, v) in wp) parms[k] = v; }
        await Exec(sql, parms);
        log.LogInformation("[REPLICATE] UPDATE {Db}.{Tbl}", db, table);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

app.MapPost("/replicate/query/delete", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    try
    {
        var sql = $"DELETE FROM `{db}`.`{table}`";
        Dictionary<string, object?>? parms = null;
        if (where.Count > 0) { var (clause, p) = BuildWhere(where); sql += " WHERE " + clause; parms = p; }
        await Exec(sql, parms);
        log.LogInformation("[REPLICATE] DELETE {Db}.{Tbl}", db, table);
        return Ok(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

app.MapPost("/replicate/snapshot", async (HttpRequest req) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    if (!body.TryGetProperty("databases", out var dbs)) return Bad("'databases' required.");
    try
    {
        foreach (var dbProp in dbs.EnumerateObject())
        {
            var db = dbProp.Name;
            await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");
            foreach (var tblProp in dbProp.Value.EnumerateObject())
            {
                var table = tblProp.Name;
                var attrs = GetStringList(tblProp.Value, "attributes");
                var recs  = tblProp.Value.TryGetProperty("records", out var recsEl)
                    ? recsEl.EnumerateArray()
                             .Select(r => JsonSerializer.Deserialize<Dictionary<string, object?>>(r.GetRawText(), jsonOpts) ?? new())
                             .ToList()
                    : new List<Dictionary<string, object?>>();
                await Exec($"DROP TABLE IF EXISTS `{db}`.`{table}`");
                var cols = new List<string> { "`id` INT AUTO_INCREMENT PRIMARY KEY" };
                foreach (var a in attrs)
                    if (!a.Equals("id", StringComparison.OrdinalIgnoreCase)) cols.Add($"`{a}` TEXT");
                await Exec($"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({string.Join(", ", cols)})");
                foreach (var rec in recs)
                {
                    var c = new List<string>(); var ph = new List<string>(); var pm = new Dictionary<string, object?>();
                    foreach (var (col, val) in rec) { c.Add($"`{col}`"); ph.Add($"@{col}"); pm[$"@{col}"] = val?.ToString(); }
                    await Exec($"INSERT INTO `{db}`.`{table}` ({string.Join(", ", c)}) VALUES ({string.Join(", ", ph)})", pm);
                }
            }
        }
        log.LogInformation("[REPLICATE] SNAPSHOT applied");
        return Ok(new { status = "snapshot applied" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ── Standard SELECT ───────────────────────────────────────────────────────

app.MapGet("/query/select", async (HttpRequest req) =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    var where = q.Keys.Where(k => k != "db" && k != "table")
                      .ToDictionary(k => k, k => (object?)(q[k].FirstOrDefault()));
    try
    {
        var sql = $"SELECT * FROM `{db}`.`{table}`";
        Dictionary<string, object?>? parms = null;
        if (where.Count > 0) { var (clause, p) = BuildWhere(where); sql += " WHERE " + clause; parms = p; }
        var rows = await QueryRows(sql, parms);
        return Results.Json(new { count = rows.Count, records = rows, servedBy = "slave-dotnet" }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  SPECIAL FEATURE: Aggregations (COUNT, AVG, MAX, MIN, SUM)
//  The C# slave's unique contribution – the Go and Python slaves don't have this.
//
//  POST /query/aggregate
//  Body: {
//    "db":    "mydb",
//    "table": "users",
//    "column": "age",
//    "functions": ["COUNT","AVG","MAX","MIN","SUM"]
//  }
// ════════════════════════════════════════════════════════════════════════════

app.MapPost("/query/aggregate", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()     ?? "";
    var table = body.GetProperty("table").GetString()  ?? "";
    var col   = body.GetProperty("column").GetString() ?? "";
    var fns   = GetStringList(body, "functions");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table) ||
        string.IsNullOrWhiteSpace(col) || fns.Count == 0)
        return Bad("'db', 'table', 'column', and 'functions' are required.");

    // Whitelist allowed functions to prevent SQL injection
    var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "COUNT","AVG","MAX","MIN","SUM" };
    var invalid = fns.Where(f => !allowed.Contains(f)).ToList();
    if (invalid.Any()) return Bad($"Unknown function(s): {string.Join(", ", invalid)}. Allowed: COUNT, AVG, MAX, MIN, SUM");

    try
    {
        var selects = fns.Select(f => $"{f.ToUpper()}(`{col}`) AS `{f.ToLower()}_{col}`");
        var sql     = $"SELECT {string.Join(", ", selects)} FROM `{db}`.`{table}`";
        var rows    = await QueryRows(sql);
        log.LogInformation("[AGGREGATE] {Fns} on {Db}.{Tbl}.{Col}", string.Join(",", fns), db, table, col);
        return Results.Json(new
        {
            db, table, column = col,
            functions = fns,
            result    = rows.FirstOrDefault(),
            servedBy  = "slave-dotnet (aggregations)"
        }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

log.LogInformation(".NET slave listening on :{Port}",
    Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort);
app.Run();