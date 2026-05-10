using System.Text;
using System.Text.Json;
using MySqlConnector;

// ════════════════════════════════════════════════════════════════════════════
//  .NET Slave Node  –  Single-file edition
//  Role       : Read replica of the Go master node
//  Technology : C# / .NET 8 / MySQL
//  Default port: 8080
// ════════════════════════════════════════════════════════════════════════════

// ── MySQL config  (match your master's config) ────────────────────────────
const string MysqlUser     = "root";
const string MysqlPassword = "root";
const string MysqlHost     = "127.0.0.1";
const string MysqlPort     = "3306";
const string SlavePort     = "8080";

var connStr = $"Server={MysqlHost};Port={MysqlPort};User ID={MysqlUser};Password={MysqlPassword};AllowPublicKeyRetrieval=true;SslMode=None;";

// ── Verify connection on startup (mirrors connectMySQL in Go slave) ────────
try
{
    using var testConn = new MySqlConnection(connStr);
    testConn.Open();
    Console.WriteLine($".NET slave connected to MySQL at {MysqlHost}:{MysqlPort}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Cannot connect to MySQL: {ex.Message}");
    return;
}

var startedAt          = DateTime.UtcNow;
long totalReplications = 0;
long totalQueries      = 0;

// ── JSON options (camelCase – matches Go's encoding/json) ─────────────────
var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
};

// ════════════════════════════════════════════════════════════════════════════
//  ASP.NET Core setup
// ════════════════════════════════════════════════════════════════════════════
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort}");
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.TimestampFormat = "[HH:mm:ss] ");

var app = builder.Build();
var log = app.Logger;

// ── Request logger ────────────────────────────────────────────────────────
app.Use(async (ctx, next) =>
{
    var sw = System.Diagnostics.Stopwatch.StartNew();
    await next();
    log.LogInformation("{Method} {Path} → {Status} ({Ms}ms)",
        ctx.Request.Method, ctx.Request.Path, ctx.Response.StatusCode, sw.ElapsedMilliseconds);
});

// ════════════════════════════════════════════════════════════════════════════
//  STORAGE HELPERS  (mirrors storage/storage.go)
// ════════════════════════════════════════════════════════════════════════════

// Open a pooled MySQL connection.
MySqlConnection Open()
{
    var c = new MySqlConnection(connStr);
    c.Open();
    return c;
}

// Execute a non-query SQL statement with named parameters.
async Task Exec(string sql, Dictionary<string, object?>? parms = null)
{
    await using var conn = Open();
    await using var cmd  = new MySqlCommand(sql, conn);
    if (parms != null)
        foreach (var (k, v) in parms)
            cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
}

// Execute SELECT → list of row dicts.
// Mirrors scanRows(): []byte → string, everything else kept as-is.
async Task<List<Dictionary<string, object?>>> QueryRows(string sql, Dictionary<string, object?>? parms = null)
{
    await using var conn   = Open();
    await using var cmd    = new MySqlCommand(sql, conn);
    if (parms != null)
        foreach (var (k, v) in parms)
            cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);

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

// Build WHERE clause + param dict from a filter map.
// Mirrors buildWhere() in storage.go.
(string Clause, Dictionary<string, object?> Params) BuildWhere(Dictionary<string, object?> where)
{
    var clauses = new List<string>();
    var parms   = new Dictionary<string, object?>();
    foreach (var (k, v) in where)
    {
        clauses.Add($"`{k}` = @wh_{k}");
        parms[$"@wh_{k}"] = v?.ToString();
    }
    return (string.Join(" AND ", clauses), parms);
}

// ── DB operations (mirrors storage.go) ───────────────────────────────────

async Task CreateDB(string db) =>
    await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");

async Task DropDB(string db) =>
    await Exec($"DROP DATABASE IF EXISTS `{db}`");

// ── Table operations ──────────────────────────────────────────────────────

// Mirrors CreateTable() in storage.go:
// always adds `id` INT AUTO_INCREMENT PRIMARY KEY first, rest as TEXT.
async Task CreateTable(string db, string table, List<string> attributes)
{
    await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");

    var colDefs = new List<string> { "`id` INT AUTO_INCREMENT PRIMARY KEY" };
    foreach (var a in attributes)
        if (!a.Equals("id", StringComparison.OrdinalIgnoreCase))
            colDefs.Add($"`{a}` TEXT");

    await Exec($"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({string.Join(", ", colDefs)})");
}

async Task DropTable(string db, string table) =>
    await Exec($"DROP TABLE IF EXISTS `{db}`.`{table}`");

// ── Record operations ─────────────────────────────────────────────────────

// Mirrors InsertRecord() in storage.go: strips "id" (auto-generated by MySQL).
async Task<long> InsertRecord(string db, string table, Dictionary<string, object?> record)
{
    var cols   = new List<string>();
    var placeh = new List<string>();
    var parms  = new Dictionary<string, object?>();

    foreach (var (col, val) in record)
    {
        if (col.Equals("id", StringComparison.OrdinalIgnoreCase)) continue;
        cols.Add($"`{col}`");
        placeh.Add($"@{col}");
        parms[$"@{col}"] = val?.ToString();
    }

    await using var conn = Open();
    await using var cmd  = new MySqlCommand(
        $"INSERT INTO `{db}`.`{table}` ({string.Join(", ", cols)}) VALUES ({string.Join(", ", placeh)})", conn);
    foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
    return cmd.LastInsertedId;
}

// Mirrors SelectRecords() in storage.go.
async Task<List<Dictionary<string, object?>>> SelectRecords(
    string db, string table, Dictionary<string, object?>? where = null)
{
    Interlocked.Increment(ref totalQueries);
    var sql = $"SELECT * FROM `{db}`.`{table}`";
    Dictionary<string, object?>? parms = null;

    if (where is { Count: > 0 })
    {
        var (clause, p) = BuildWhere(where);
        sql  += " WHERE " + clause;
        parms = p;
    }
    return await QueryRows(sql, parms);
}

// Mirrors UpdateRecords() in storage.go.
async Task<int> UpdateRecords(
    string db, string table,
    Dictionary<string, object?> where,
    Dictionary<string, object?> set)
{
    var setClauses = set.Keys.Select(k => $"`{k}` = @set_{k}").ToList();
    var parms      = new Dictionary<string, object?>();
    foreach (var (k, v) in set)   parms[$"@set_{k}"] = v?.ToString();

    var sql = $"UPDATE `{db}`.`{table}` SET {string.Join(", ", setClauses)}";
    if (where.Count > 0)
    {
        var (clause, whereP) = BuildWhere(where);
        sql += " WHERE " + clause;
        foreach (var (k, v) in whereP) parms[k] = v;
    }

    await using var conn = Open();
    await using var cmd  = new MySqlCommand(sql, conn);
    foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
    return (int)cmd.LastInsertedId; // RowsAffected not exposed; use AffectedRows via cast below
}

// Mirrors DeleteRecords() in storage.go.
async Task DeleteRecords(string db, string table, Dictionary<string, object?> where)
{
    var sql   = $"DELETE FROM `{db}`.`{table}`";
    Dictionary<string, object?>? parms = null;
    if (where.Count > 0)
    {
        var (clause, p) = BuildWhere(where);
        sql  += " WHERE " + clause;
        parms = p;
    }
    await Exec(sql, parms);
}

// ── JSON response helpers ─────────────────────────────────────────────────
IResult Respond(object data)  => Results.Json(new { success = true,  data },        jsonOpts);
IResult Fail(string msg)      => Results.Json(new { success = false, error = msg },  jsonOpts, statusCode: 500);
IResult Bad(string msg)       => Results.Json(new { success = false, error = msg },  jsonOpts, statusCode: 400);

Dictionary<string, object?> GetDict(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? JsonSerializer.Deserialize<Dictionary<string, object?>>(el.GetRawText(), jsonOpts) ?? new()
        : new();

List<string> GetStringList(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? el.EnumerateArray().Select(x => x.GetString() ?? "").ToList()
        : new();

// ════════════════════════════════════════════════════════════════════════════
//  GET /health
//  Master polls this every 10 s (StartHealthChecker interval).
// ════════════════════════════════════════════════════════════════════════════
app.MapGet("/health", () => Results.Json(new
{
    status = "ok",
    role   = "slave-dotnet",
}, jsonOpts));

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/table/create
//  Sent by master after POST /table/create succeeds.
//  Body: { "db":"mydb", "table":"users", "attributes":["name","age"] }
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/table/create", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var attrs = GetStringList(body, "attributes");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    if (attrs.Count == 0)
        return Bad("at least one attribute is required.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        await CreateTable(db, table, attrs);
        log.LogInformation("[REPLICATE] CREATE_TABLE {Db}.{Tbl}", db, table);
        return Respond(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/table/drop
//  Body: { "db":"mydb", "table":"users" }
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/table/drop", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        await DropTable(db, table);
        log.LogInformation("[REPLICATE] DROP_TABLE {Db}.{Tbl}", db, table);
        return Respond(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/query/insert
//  Body: { "db":"mydb", "table":"users", "record":{"id":1,"name":"Ali","age":"20"} }
//  The master sends the MySQL-generated id so both sides stay in sync.
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/query/insert", async (HttpRequest req) =>
{
    var body   = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db     = body.GetProperty("db").GetString()    ?? "";
    var table  = body.GetProperty("table").GetString() ?? "";
    var record = GetDict(body, "record");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    if (record.Count == 0)
        return Bad("'record' cannot be empty.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        await InsertRecord(db, table, record);
        log.LogInformation("[REPLICATE] INSERT → {Db}.{Tbl}", db, table);
        return Respond(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/query/update
//  Body: { "db":"mydb", "table":"users", "where":{"id":1}, "set":{"age":"21"} }
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/query/update", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    var set   = GetDict(body, "set");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    if (set.Count == 0)
        return Bad("'set' cannot be empty.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        await UpdateRecords(db, table, where, set);
        log.LogInformation("[REPLICATE] UPDATE {Db}.{Tbl}", db, table);
        return Respond(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/query/delete
//  Body: { "db":"mydb", "table":"users", "where":{"id":1} }
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/query/delete", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        await DeleteRecords(db, table, where);
        log.LogInformation("[REPLICATE] DELETE {Db}.{Tbl}", db, table);
        return Respond(new { status = "replicated" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /replicate/snapshot
//  Master calls this when this slave recovers after downtime (buildSnapshot).
//  Body: { "databases": { "mydb": { "users": { "attributes":[...], "records":[...] } } } }
//  Mirrors replicateSnapshot() in Go slave.
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/replicate/snapshot", async (HttpRequest req) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    if (!body.TryGetProperty("databases", out var dbs))
        return Bad("'databases' field is required.");

    Interlocked.Increment(ref totalReplications);
    try
    {
        foreach (var dbProp in dbs.EnumerateObject())
        {
            var db = dbProp.Name;
            await CreateDB(db);

            foreach (var tblProp in dbProp.Value.EnumerateObject())
            {
                var table = tblProp.Name;
                var attrs = GetStringList(tblProp.Value, "attributes");
                var recs  = tblProp.Value.TryGetProperty("records", out var recsEl)
                    ? recsEl.EnumerateArray()
                             .Select(r => JsonSerializer.Deserialize<Dictionary<string, object?>>(
                                 r.GetRawText(), jsonOpts) ?? new())
                             .ToList()
                    : new List<Dictionary<string, object?>>();

                // Wipe and rebuild (mirrors ReplaceTable in storage.go)
                await DropTable(db, table);
                await CreateTable(db, table, attrs);
                foreach (var rec in recs)
                    await InsertRecord(db, table, rec);
            }
        }
        log.LogInformation("[REPLICATE] SNAPSHOT applied");
        return Respond(new { status = "snapshot applied" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  GET /query/select?db=mydb&table=users[&col=val...]
//  Same interface as Go slave — any extra query param becomes a WHERE filter.
//  Master also calls this for read queries it delegates to slaves.
// ════════════════════════════════════════════════════════════════════════════
app.MapGet("/query/select", async (HttpRequest req) =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("Query params 'db' and 'table' are required.");

    // Every param that is NOT "db" or "table" becomes a WHERE condition
    // (mirrors localSelect in Go slave)
    var where = q.Keys
        .Where(k => k != "db" && k != "table")
        .ToDictionary(k => k, k => (object?)(q[k].FirstOrDefault()));

    try
    {
        var rows = await SelectRecords(db, table, where.Count > 0 ? where : null);
        return Results.Json(new
        {
            count    = rows.Count,
            records  = rows,
            servedBy = $"slave-dotnet :{Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort}"
        }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  POST /query/search
//  Special .NET slave feature: full-text keyword search across ALL TEXT
//  columns of a table. Go master/slave don't have this endpoint.
//  Body: { "db":"mydb", "table":"users", "keyword":"ali" }
// ════════════════════════════════════════════════════════════════════════════
app.MapPost("/query/search", async (HttpRequest req) =>
{
    var body    = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db      = body.GetProperty("db").GetString()      ?? "";
    var table   = body.GetProperty("table").GetString()   ?? "";
    var keyword = body.GetProperty("keyword").GetString() ?? "";

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table) || string.IsNullOrWhiteSpace(keyword))
        return Bad("'db', 'table', and 'keyword' are required.");

    try
    {
        // Get TEXT columns from information_schema (same source as storage.go)
        var colRows = await QueryRows(
            @"SELECT column_name FROM information_schema.columns
              WHERE table_schema = @db AND table_name = @tbl
              AND data_type = 'text'
              ORDER BY ordinal_position",
            new Dictionary<string, object?> { ["@db"] = db, ["@tbl"] = table });

        var textCols = colRows.Select(r => r["column_name"]?.ToString() ?? "")
                              .Where(c => !string.IsNullOrEmpty(c))
                              .ToList();

        if (textCols.Count == 0)
            return Results.Json(new { count = 0, records = Array.Empty<object>(), servedBy = "slave-dotnet" }, jsonOpts);

        // Build: WHERE col1 LIKE @kw OR col2 LIKE @kw ...
        var likeClauses = string.Join(" OR ", textCols.Select(c => $"`{c}` LIKE @kw"));
        var sql = $"SELECT * FROM `{db}`.`{table}` WHERE {likeClauses};";

        var rows = await QueryRows(sql, new Dictionary<string, object?> { ["@kw"] = $"%{keyword}%" });

        log.LogInformation("[SEARCH] '{Kw}' in {Db}.{Tbl} → {N} row(s)", keyword, db, table, rows.Count);
        return Results.Json(new { count = rows.Count, records = rows, servedBy = "slave-dotnet" }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  Startup banner
// ════════════════════════════════════════════════════════════════════════════
log.LogInformation(".NET slave listening on :{Port}",
    Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort);

app.Run();