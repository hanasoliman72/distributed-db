using System.Text.Json;
using MySqlConnector;

const string MysqlUser     = "root";
const string MysqlPassword = "root";
const string MysqlHost     = "127.0.0.1";
const string MysqlPort     = "3306";
const string SlavePort     = "8081";

const string SelfAddr   = "http://127.0.0.1:8081";
const string MasterAddr = "http://127.0.0.1:8080";

var peers = new List<string>
{
    MasterAddr,
    "http://127.0.0.1:8082",// Go slave
    "http://127.0.0.1:8083",// Python slave
};

var connStr = $"Server={MysqlHost};Port={MysqlPort};User ID={MysqlUser};Password={MysqlPassword};AllowPublicKeyRetrieval=true;SslMode=None;";

// ── Role state (thread-safe) ──────────────────────────────────────────────
var stateLock       = new object();
bool masterDown     = false;
bool isActingMaster = false;

void SetMasterDown(bool down, ILogger logger)
{
    lock (stateLock)
    {
        if (down && !masterDown)
        {
            logger.LogWarning("[WATCHER] Master {Addr} unreachable — promoting self to acting master", MasterAddr);
            isActingMaster = true;
        }
        else if (!down && masterDown)
        {
            logger.LogInformation("[WATCHER] Master {Addr} is back online — reverting to slave role", MasterAddr);
            isActingMaster = false;
        }
        masterDown = down;
    }
}

bool IsMasterDown()  { lock (stateLock) return masterDown; }
bool CanManageDb()   { lock (stateLock) return isActingMaster; }
string SelfRole()    { lock (stateLock) return isActingMaster ? "slave-dotnet (acting master)" : "slave-dotnet"; }

// ── Shared HTTP client ────────────────────────────────────────────────────
var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

// ── MySQL helpers ─────────────────────────────────────────────────────────
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

// ── JSON helpers ──────────────────────────────────────────────────────────
var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
};

Dictionary<string, object?> GetDict(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? JsonSerializer.Deserialize<Dictionary<string, object?>>(el.GetRawText(), jsonOpts) ?? new()
        : new();

List<string> GetStringList(JsonElement root, string prop) =>
    root.TryGetProperty(prop, out var el)
        ? el.EnumerateArray().Select(x => x.GetString() ?? "").ToList()
        : new();

IResult Ok(object data)       => Results.Json(new { success = true,  data }, jsonOpts);
IResult Fail(string msg)      => Results.Json(new { success = false, error = msg }, jsonOpts, statusCode: 500);
IResult Bad(string msg)       => Results.Json(new { success = false, error = msg }, jsonOpts, statusCode: 400);
IResult Forbidden(string msg) => Results.Json(new { success = false, error = msg, tip = $"Send this request to the master at {MasterAddr}" }, jsonOpts, statusCode: 403);

// ── Broadcaster (fire-and-forget POST to all peers) ───────────────────────
void Broadcast(string path, object payload, ILogger logger)
{
    var body = JsonSerializer.Serialize(payload, jsonOpts);
    foreach (var peer in peers)
    {
        if (peer == SelfAddr) continue;
        if (peer == MasterAddr && IsMasterDown())
        {
            logger.LogInformation("[BROADCAST] Skipping down master {Addr}", peer);
            continue;
        }

        var url = peer; // capture for lambda
        _ = Task.Run(async () =>
        {
            try
            {
                var content  = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
                var response = await httpClient.PostAsync(url + path, content);
                logger.LogInformation("[BROADCAST] POST {Url}{Path} → {Status}", url, path, (int)response.StatusCode);
                if (url == MasterAddr) SetMasterDown(false, logger);
            }
            catch (Exception ex)
            {
                logger.LogWarning("[BROADCAST] POST {Url}{Path} failed: {Err}", url, path, ex.Message);
                if (url == MasterAddr) SetMasterDown(true, logger);
            }
        });
    }
}

// ── Build app ─────────────────────────────────────────────────────────────
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort}");
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.TimestampFormat = "[HH:mm:ss] ");
var app = builder.Build();
var log = app.Logger;

// ── Verify MySQL on startup ───────────────────────────────────────────────
try
{
    using var testConn = new MySqlConnection(connStr);
    testConn.Open();
    log.LogInformation("[startup] Connected to MySQL at {Host}:{Port}", MysqlHost, MysqlPort);
}
catch (Exception ex)
{
    log.LogError("Cannot connect to MySQL: {Err}", ex.Message);
    return;
}

// ── Background master watcher (every 5 s, same as Python slave) ───────────
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(5));
        try
        {
            var resp = await httpClient.GetAsync(MasterAddr + "/health");
            resp.EnsureSuccessStatusCode();
            SetMasterDown(false, log);
        }
        catch
        {
            SetMasterDown(true, log);
        }
    }
});

// ════════════════════════════════════════════════════════════════════════════
//  GET /health
// ════════════════════════════════════════════════════════════════════════════

app.MapGet("/health", () =>
    Results.Json(new { status = "ok", role = SelfRole() }, jsonOpts));

// ════════════════════════════════════════════════════════════════════════════
//  REPLICATION RECEIVERS  –  called by master / peers, apply locally only
// ════════════════════════════════════════════════════════════════════════════

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
            if (!a.Equals("id", StringComparison.OrdinalIgnoreCase)) cols.Add($"`{a}` TEXT");
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
        foreach (var (col, val) in record) { cols.Add($"`{col}`"); phs.Add($"@{col}"); parms[$"@{col}"] = val?.ToString(); }
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

// ── Database ──────────────────────────────────────────────────────────────

// POST /query/db/create
// Body: { "db": "mydb" }
app.MapPost("/query/db/create", async (HttpRequest req) =>
{
    if (!CanManageDb()) return Forbidden("Database create/drop is only allowed on the master node.");

    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    if (string.IsNullOrWhiteSpace(db)) return Bad("'db' is required.");
    try
    {
        await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");
        log.LogInformation("[DB/CREATE] Created database {Db}", db);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/db/create", new { db, origin = SelfAddr }, log);
    return Results.Json(new { message = $"database '{db}' created", servedBy = SelfRole() }, jsonOpts, statusCode: 201);
});

// DELETE /query/db/drop
// Body: { "db": "mydb" }
app.MapDelete("/query/db/drop", async (HttpRequest req) =>
{
    if (!CanManageDb()) return Forbidden("Database create/drop is only allowed on the master node.");

    var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db   = body.GetProperty("db").GetString() ?? "";
    if (string.IsNullOrWhiteSpace(db)) return Bad("'db' is required.");
    try
    {
        await Exec($"DROP DATABASE IF EXISTS `{db}`");
        log.LogInformation("[DB/DROP] Dropped database {Db}", db);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/db/drop", new { db, origin = SelfAddr }, log);
    return Results.Json(new { message = $"database '{db}' dropped", servedBy = SelfRole() }, jsonOpts);
});

// ── Tables ────────────────────────────────────────────────────────────────

// POST /query/table/create
// Body: { "db":"mydb", "table":"users", "attributes":["name","age"] }
app.MapPost("/query/table/create", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var attrs = GetStringList(body, "attributes");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table) || attrs.Count == 0)
        return Bad("'db', 'table', and 'attributes' are required.");
    try
    {
        await Exec($"CREATE DATABASE IF NOT EXISTS `{db}`");
        var cols = new List<string> { "`id` INT AUTO_INCREMENT PRIMARY KEY" };
        foreach (var a in attrs)
            if (!a.Equals("id", StringComparison.OrdinalIgnoreCase)) cols.Add($"`{a}` TEXT");
        await Exec($"CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ({string.Join(", ", cols)})");
        log.LogInformation("[TABLE/CREATE] {Db}.{Tbl}", db, table);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/table/create", new { db, table, attributes = attrs, origin = SelfAddr }, log);
    return Results.Json(new { message = $"table '{table}' created", servedBy = SelfRole() }, jsonOpts, statusCode: 201);
});

// DELETE /query/table/drop
// Body: { "db":"mydb", "table":"users" }
app.MapDelete("/query/table/drop", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    try
    {
        await Exec($"DROP TABLE IF EXISTS `{db}`.`{table}`");
        log.LogInformation("[TABLE/DROP] {Db}.{Tbl}", db, table);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/table/drop", new { db, table, origin = SelfAddr }, log);
    return Results.Json(new { message = $"table '{table}' dropped", servedBy = SelfRole() }, jsonOpts);
});

// ── Rows ──────────────────────────────────────────────────────────────────

// GET /query/select?db=mydb&table=users[&col=val...]
app.MapGet("/query/select", async (HttpRequest req) =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";
    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");

    var where = q.Keys
                 .Where(k => k != "db" && k != "table")
                 .ToDictionary(k => k, k => (object?)(q[k].FirstOrDefault()));
    try
    {
        var sql = $"SELECT * FROM `{db}`.`{table}`";
        Dictionary<string, object?>? parms = null;
        if (where.Count > 0) { var (clause, p) = BuildWhere(where); sql += " WHERE " + clause; parms = p; }
        var rows = await QueryRows(sql, parms);
        return Results.Json(new { count = rows.Count, records = rows, servedBy = SelfRole() }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// POST /query/insert
// Body: { "db":"mydb", "table":"users", "record":{"name":"Hana","age":"22"} }
app.MapPost("/query/insert", async (HttpRequest req) =>
{
    var body   = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db     = body.GetProperty("db").GetString()    ?? "";
    var table  = body.GetProperty("table").GetString() ?? "";
    var record = GetDict(body, "record");
    record.Remove("id"); // let MySQL auto-increment

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table)) return Bad("'db' and 'table' are required.");
    if (record.Count == 0) return Bad("'record' cannot be empty.");

    long generatedId;
    try
    {
        var cols  = new List<string>(); var phs = new List<string>(); var parms = new Dictionary<string, object?>();
        foreach (var (col, val) in record) { cols.Add($"`{col}`"); phs.Add($"@{col}"); parms[$"@{col}"] = val?.ToString(); }
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(
            $"INSERT INTO `{db}`.`{table}` ({string.Join(", ", cols)}) VALUES ({string.Join(", ", phs)})", conn);
        foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
        generatedId = cmd.LastInsertedId;
        log.LogInformation("[INSERT] {Db}.{Tbl} id={Id}", db, table, generatedId);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    // broadcast with real id so all peers store the same row
    var broadcastRecord = new Dictionary<string, object?>(record) { ["id"] = generatedId };
    Broadcast("/replicate/query/insert", new { db, table, record = broadcastRecord, origin = SelfAddr }, log);
    return Results.Json(new { message = "record inserted", generatedId, servedBy = SelfRole() }, jsonOpts, statusCode: 201);
});

// PUT /query/update
// Body: { "db":"mydb", "table":"users", "where":{"id":1}, "set":{"age":"21"} }
app.MapPut("/query/update", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");
    var set   = GetDict(body, "set");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table)) return Bad("'db' and 'table' are required.");
    if (set.Count == 0) return Bad("'set' cannot be empty.");

    int affected;
    try
    {
        var setClauses = set.Keys.Select(k => $"`{k}` = @set_{k}").ToList();
        var parms      = new Dictionary<string, object?>();
        foreach (var (k, v) in set) parms[$"@set_{k}"] = v?.ToString();
        var sql = $"UPDATE `{db}`.`{table}` SET {string.Join(", ", setClauses)}";
        if (where.Count > 0) { var (clause, wp) = BuildWhere(where); sql += " WHERE " + clause; foreach (var (k, v) in wp) parms[k] = v; }
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(sql, conn);
        foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        affected = await cmd.ExecuteNonQueryAsync();
        log.LogInformation("[UPDATE] {Db}.{Tbl} rows={N}", db, table, affected);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/query/update", new { db, table, where, set, origin = SelfAddr }, log);
    return Results.Json(new { message = "update complete", recordsUpdated = affected, servedBy = SelfRole() }, jsonOpts);
});

// DELETE /query/delete
// Body: { "db":"mydb", "table":"users", "where":{"id":1} }
app.MapDelete("/query/delete", async (HttpRequest req) =>
{
    var body  = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body);
    var db    = body.GetProperty("db").GetString()    ?? "";
    var table = body.GetProperty("table").GetString() ?? "";
    var where = GetDict(body, "where");

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table)) return Bad("'db' and 'table' are required.");

    int affected;
    try
    {
        var sql = $"DELETE FROM `{db}`.`{table}`";
        Dictionary<string, object?>? parms = null;
        if (where.Count > 0) { var (clause, p) = BuildWhere(where); sql += " WHERE " + clause; parms = p; }
        await using var conn = Open();
        await using var cmd  = new MySqlCommand(sql, conn);
        if (parms != null) foreach (var (k, v) in parms) cmd.Parameters.AddWithValue(k, v ?? DBNull.Value);
        affected = await cmd.ExecuteNonQueryAsync();
        log.LogInformation("[DELETE] {Db}.{Tbl} rows={N}", db, table, affected);
    }
    catch (Exception ex) { return Fail(ex.Message); }

    Broadcast("/replicate/query/delete", new { db, table, where, origin = SelfAddr }, log);
    return Results.Json(new { message = "delete complete", recordsDeleted = affected, servedBy = SelfRole() }, jsonOpts);
});

// ── Full-text search across ALL columns (mirrors Python slave) ────────────

// GET /query/search?db=mydb&table=users&q=ali
app.MapGet("/query/search", async (HttpRequest req) =>
{
    var q     = req.Query;
    var db    = q["db"].FirstOrDefault()    ?? "";
    var table = q["table"].FirstOrDefault() ?? "";
    var term  = (q["q"].FirstOrDefault()    ?? "").Trim();

    if (string.IsNullOrWhiteSpace(db) || string.IsNullOrWhiteSpace(table))
        return Bad("'db' and 'table' are required.");
    if (string.IsNullOrWhiteSpace(term))
        return Bad("'q' (search term) is required.");

    try
    {
        var allRows   = await QueryRows($"SELECT * FROM `{db}`.`{table}`");
        var termLower = term.ToLowerInvariant();
        var matched   = allRows
            .Where(row => row.Values.Any(v => v != null && v.ToString()!.ToLowerInvariant().Contains(termLower)))
            .ToList();
        log.LogInformation("[SEARCH] {Db}.{Tbl} q='{Term}' → {N} row(s)", db, table, term, matched.Count);
        return Results.Json(new { searchTerm = term, count = matched.Count, records = matched, servedBy = SelfRole() }, jsonOpts);
    }
    catch (Exception ex) { return Fail(ex.Message); }
});

// ════════════════════════════════════════════════════════════════════════════
//  Startup
// ════════════════════════════════════════════════════════════════════════════

log.LogInformation(".NET slave listening on :{Port}", Environment.GetEnvironmentVariable("SLAVE_PORT") ?? SlavePort);
log.LogInformation("Master: {MasterAddr} | Peers: {Peers}", MasterAddr, string.Join(", ", peers));
app.Run();