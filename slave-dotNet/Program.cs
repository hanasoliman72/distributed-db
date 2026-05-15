// slave-dotnet/Program.cs
//
// .NET slave node (Shard C).
// Security  : every /shard/* request must carry X-Gateway-Token (HMAC-SHA256).
// Resilience: writes mirror to <db>_replica schema; reads fall back to it.

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MySqlConnector;

// ── Config ────────────────────────────────────────────────────────────────
var mysqlHost = Environment.GetEnvironmentVariable("MYSQL_HOST") ?? "127.0.0.1";
var mysqlPort = Environment.GetEnvironmentVariable("MYSQL_PORT") ?? "3306";
var mysqlUser = Environment.GetEnvironmentVariable("MYSQL_USER") ?? "root";
var mysqlPass = Environment.GetEnvironmentVariable("MYSQL_PASSWORD") ?? "root";
var slavePort = Environment.GetEnvironmentVariable("SLAVE_PORT") ?? "8083";
var secretStr = Environment.GetEnvironmentVariable("SLAVE_SHARED_SECRET") ?? "Hana-1234";
var secret    = Encoding.UTF8.GetBytes(secretStr);


var connStr = $"Server={mysqlHost};Port={mysqlPort};User ID={mysqlUser};Password={mysqlPass};" +
              "AllowPublicKeyRetrieval=true;SslMode=None;";

var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
};

// ── HMAC Auth ─────────────────────────────────────────────────────────────

bool VerifyToken(string token)
{
    try
    {
        var parts = token.Split('|', 2);
if (parts.Length != 2) return false;

var nonce  = parts[0];
var gotSig = parts[1];

using var mac = new HMACSHA256(secret);

var wantBytes = mac.ComputeHash(
    Encoding.UTF8.GetBytes(nonce)
);
        var wantSig    = Convert.ToHexString(wantBytes).ToLower();
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(wantSig), Encoding.UTF8.GetBytes(gotSig));
    }
    catch { return false; }
}

IResult Forbidden(string msg) =>
    Results.Json(new { error = msg }, jsonOpts, statusCode: 403);

// Minimal middleware helper: checks token and runs handler.
async Task<IResult> Guarded(HttpRequest req, Func<Task<IResult>> handler)
{
    var token = req.Headers["X-Gateway-Token"].FirstOrDefault() ?? "";
    if (!VerifyToken(token)) return Forbidden("invalid or missing gateway token");
    return await handler();
}

// ── MySQL helpers ─────────────────────────────────────────────────────────

string Replica(string db) => db + "_replica";

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

async Task<List<Dictionary<string, object?>>> Query(string sql, Dictionary<string, object?>? p = null)
{
    await using var conn   = Open();
    await using var cmd    = new MySqlCommand(sql, conn);
    if (p != null) foreach (var (k, v) in p) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
    await using var rdr    = await cmd.ExecuteReaderAsync();
    var rows = new List<Dictionary<string, object?>>();
    while (await rdr.ReadAsync())
    {
        var row = new Dictionary<string, object?>();
        for (int i = 0; i < rdr.FieldCount; i++)
            row[rdr.GetName(i)] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
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

bool IsValidIdentifier(string s) =>
    !string.IsNullOrEmpty(s) && s.Length <= 64 && 
    System.Text.RegularExpressions.Regex.IsMatch(s, @"^[a-zA-Z0-9_-]+$");

IResult Ok(object data)  => Results.Json(data, jsonOpts);
IResult Fail(string msg) => Results.Json(new { error = msg }, jsonOpts, statusCode: 500);
IResult Bad(string msg)  => Results.Json(new { error = msg }, jsonOpts, statusCode: 400);

// ── Build app ─────────────────────────────────────────────────────────────
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{slavePort}");
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.TimestampFormat = "[HH:mm:ss] ");
var app = builder.Build();
var log = app.Logger;

// Verify MySQL on startup.
try { using var t = Open(); log.LogInformation("[slave-dotnet] MySQL connected"); }
catch (Exception ex) { log.LogError("Cannot connect MySQL: {E}", ex.Message); return; }

// ── Health (no auth) ──────────────────────────────────────────────────────

app.MapGet("/health", () =>
    Results.Json(new { status = "ok", role = "slave-dotnet" }, jsonOpts));

// ── DB DDL ────────────────────────────────────────────────────────────────

app.MapPost("/shard/db/create", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    if (string.IsNullOrWhiteSpace(db) || !IsValidIdentifier(db)) return Bad("invalid db name");
    try
    {
        foreach (var s in new[] { db, Replica(db) })
            await Exec($"CREATE DATABASE IF NOT EXISTS `{s}`");
        return Ok(new { status = "ok" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
}));

app.MapDelete("/shard/db/drop", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    foreach (var s in new[] { db, Replica(db) })
        try { await Exec($"DROP DATABASE IF EXISTS `{s}`"); } catch { }
    return Ok(new { status = "ok" });
}));

// ── Table DDL ─────────────────────────────────────────────────────────────

app.MapPost("/shard/table/create", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var attrs = GetStringList(body, "attributes");
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table) || !IsValidIdentifier(db) || !IsValidIdentifier(table))
        return Bad("invalid db or table name");
    var cols  = new List<string> { "`id` INT AUTO_INCREMENT PRIMARY KEY" };
    foreach (var a in attrs)
    {
        if (!IsValidIdentifier(a)) return Bad($"invalid attribute name: {a}");
        if (!a.Equals("id", StringComparison.OrdinalIgnoreCase)) cols.Add($"`{a}` TEXT");
    }
    var colDef = string.Join(", ", cols);
    try
    {
        foreach (var s in new[] { db, Replica(db) })
        {
            await Exec($"CREATE DATABASE IF NOT EXISTS `{s}`");
            await Exec($"CREATE TABLE IF NOT EXISTS `{s}`.`{table}` ({colDef})");
        }
        return Ok(new { status = "ok" });
    }
    catch (Exception ex) { return Fail(ex.Message); }
}));

app.MapDelete("/shard/table/drop", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    foreach (var s in new[] { db, Replica(db) })
        try { await Exec($"DROP TABLE IF EXISTS `{s}`.`{table}`"); } catch { }
    return Ok(new { status = "ok" });
}));

// ── INSERT ────────────────────────────────────────────────────────────────

app.MapPost("/shard/query/insert", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body   = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db     = body.GetProperty("db").GetString()    ?? "";
    var table  = body.GetProperty("table").GetString() ?? "";
    var record = GetDict(body, "record");
    record.Remove("id");
    if (record.Count == 0) return Bad("record cannot be empty");

    var cols  = new List<string>(); var phs = new List<string>(); var parms = new Dictionary<string, object?>();
    foreach (var (col, val) in record) { cols.Add($"`{col}`"); phs.Add($"@{col}"); parms[$"@{col}"] = val?.ToString(); }
    try
    {
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(
            $"INSERT INTO `{db}`.`{table}` ({string.Join(", ", cols)}) VALUES ({string.Join(", ", phs)})", conn);
        foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
        var genId = cmd.LastInsertedId;

        // Mirror to replica.
        var repRecord = new Dictionary<string, object?>(record) { ["id"] = genId };
        var rc = repRecord.Keys.Select(k => $"`{k}`").ToList();
        var rp = repRecord.Keys.Select(k => $"@rep_{k}").ToList();
        var rm = repRecord.ToDictionary(kv => $"@rep_{kv.Key}", kv => (object?)(kv.Value?.ToString()));
        await using var conn2 = Open();
        await using var cmd2  = new MySqlCommand(
            $"INSERT IGNORE INTO `{Replica(db)}`.`{table}` ({string.Join(", ", rc)}) VALUES ({string.Join(", ", rp)})", conn2);
        foreach (var (k, v) in rm) cmd2.Parameters.AddWithValue(k, v ?? DBNull.Value);
        try { await cmd2.ExecuteNonQueryAsync(); } catch (Exception rex) { log.LogWarning($"[replica] failed to mirror insert to {Replica(db)}.{table}: {rex.Message}"); }

        log.LogInformation("[INSERT] {Db}.{Tbl} id={Id}", db, table, genId);
        return Results.Json(new { message = "record inserted", generated_id = genId }, jsonOpts, statusCode: 201);
    }
    catch (Exception ex) { return Fail(ex.Message); }
}));

// ── SELECT ────────────────────────────────────────────────────────────────

app.MapGet("/shard/query/select", async (HttpRequest req) => await Guarded(req, async () =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table)) return Bad("db and table required");

    var where = q.Keys.Where(k => k != "db" && k != "table")
                       .ToDictionary(k => k, k => (object?)(q[k].FirstOrDefault()));
    string sqlStr = $"SELECT * FROM `{db}`.`{table}`";
    Dictionary<string, object?>? parms = null;
    if (where.Count > 0) { var (clause, p) = BuildWhere(where); sqlStr += " WHERE " + clause; parms = p; }

    try
    {
        var rows = await Query(sqlStr, parms);
        return Ok(new { count = rows.Count, records = rows });
    }
    catch
    {
        // Fallback to replica.
        try
        {
            var repSql  = sqlStr.Replace($"`{db}`.", $"`{Replica(db)}`.");
            var rows    = await Query(repSql, parms);
            return Ok(new { count = rows.Count, records = rows, source = "replica" });
        }
        catch (Exception ex2) { return Fail(ex2.Message); }
    }
}));

// ── UPDATE ────────────────────────────────────────────────────────────────

app.MapPut("/shard/query/update", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    var set   = GetDict(body, "set");
    if (set.Count == 0) return Bad("set required");

    var setClauses = set.Keys.Select(k => $"`{k}` = @set_{k}").ToList();
    var parms      = new Dictionary<string, object?>();
    foreach (var (k, v) in set) parms[$"@set_{k}"] = v?.ToString();
    var sqlStr = $"UPDATE `{db}`.`{table}` SET {string.Join(", ", setClauses)}";
    if (where.Count > 0) { var (clause, wp) = BuildWhere(where); sqlStr += " WHERE " + clause; foreach (var (k, v) in wp) parms[k] = v; }

    try
    {
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(sqlStr, conn);
        foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        var affected = await cmd.ExecuteNonQueryAsync();

        // Best-effort replica update.
        try { await Exec(sqlStr.Replace($"`{db}`.", $"`{Replica(db)}`."), parms); }
        catch (Exception rex) { log.LogWarning($"[replica] failed to update replica {db}: {rex.Message}"); }

        return Ok(new { message = "update complete", records_updated = affected });
    }
    catch (Exception ex) { return Fail(ex.Message); }
}));

// ── DELETE ────────────────────────────────────────────────────────────────

app.MapDelete("/shard/query/delete", async (HttpRequest req) => await Guarded(req, async () =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    var sqlStr = $"DELETE FROM `{db}`.`{table}`";
    Dictionary<string, object?>? parms = null;
    if (where.Count > 0) { var (clause, p) = BuildWhere(where); sqlStr += " WHERE " + clause; parms = p; }

    try
    {
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(sqlStr, conn);
        if (parms != null) foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        var affected = await cmd.ExecuteNonQueryAsync();

        try { await Exec(sqlStr.Replace($"`{db}`.", $"`{Replica(db)}`."), parms); }
        catch (Exception rex) { log.LogWarning($"[replica] failed to delete from replica {db}: {rex.Message}"); }

        return Ok(new { message = "delete complete", records_deleted = affected });
    }
    catch (Exception ex) { return Fail(ex.Message); }
}));

// ── SEARCH ────────────────────────────────────────────────────────────────

app.MapGet("/shard/query/search", async (HttpRequest req) => await Guarded(req, async () =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";
    var term  = (q["q"].FirstOrDefault()    ?? "").Trim();
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table) || string.IsNullOrWhiteSpace(term))
        return Bad("db, table, q required");

    List<Dictionary<string, object?>> allRows;
    try   { allRows = await Query($"SELECT * FROM `{db}`.`{table}`"); }
    catch { allRows = await Query($"SELECT * FROM `{Replica(db)}`.`{table}`"); }

    var termLower = term.ToLowerInvariant();
    var matched   = allRows.Where(row =>
        row.Values.Any(v => v != null && v.ToString()!.ToLowerInvariant().Contains(termLower))).ToList();

    return Ok(new { search_term = term, count = matched.Count, records = matched });
}));

// ── Run ───────────────────────────────────────────────────────────────────
log.LogInformation(".NET slave listening on :{Port}", slavePort);
app.Run();