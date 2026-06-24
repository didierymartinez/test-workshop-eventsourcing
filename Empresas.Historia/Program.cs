using Microsoft.Extensions.DependencyInjection;
using Marten;
using JasperFx.Events;

// --- Configuración de Marten (en el arranque), con el Postgres de «Docker y PostgreSQL» ---
var services = new ServiceCollection();

services.AddMarten(options =>
{
    options.Connection("Host=localhost;Port=5432;Database=gestion_eventstore;Username=gestion;Password=dev_local_pwd");
    options.UseSystemTextJsonForSerialization();
    options.Events.StreamIdentity   = StreamIdentity.AsString;          // la llave del stream es string (tu Id)
    options.Events.EventNamingStyle = EventNamingStyle.SmarterTypeName; // guarda el nombre del tipo de evento
}).UseLightweightSessions();

// EL SWAP: una línea — antes era InMemoryEventStore
services.AddScoped<IEventStore, MartenEventStore>();

services.AddScoped<RegistrarEmpresaHandler>();
services.AddScoped<CambiarPlanHandler>();
services.AddScoped<SuspenderHandler>();

await using var proveedor = services.BuildServiceProvider();

// Misma operación de «El almacén directo», ahora contra Postgres real vía Marten:
await using (var scope = proveedor.CreateAsyncScope())
{
    var sp = scope.ServiceProvider;
    await sp.GetRequiredService<RegistrarEmpresaHandler>()
            .HandleAsync(new RegistrarEmpresa("emp-7", "Constructora Andes", "Básico"));
}
await using (var scope = proveedor.CreateAsyncScope())
{
    await scope.ServiceProvider.GetRequiredService<CambiarPlanHandler>()
            .HandleAsync(new CambiarPlanDeEmpresa("emp-7", "Premium"));
}
await using (var scope = proveedor.CreateAsyncScope())
{
    await scope.ServiceProvider.GetRequiredService<SuspenderHandler>()
            .HandleAsync(new SuspenderEmpresa("emp-7", "falta de pago"));
}

await using (var scope = proveedor.CreateAsyncScope())
{
    var store = scope.ServiceProvider.GetRequiredService<IEventStore>();
    var empresa = await store.GetAggregateRootAsync<Empresa>("emp-7");
    Console.WriteLine($"{empresa!.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}, versión {empresa.Version}");
}

// ===================== El contrato =====================
public interface IEventStore
{
    void StartStream(AggregateRoot agregado);
    Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot, new();
    void AppendEvent(string id, object hecho);
    Task SaveChangesAsync(CancellationToken ct = default);
    Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot;
}

// ===================== Implementación 1: en memoria =====================
public class InMemoryEventStore : IEventStore
{
    private readonly Dictionary<string, List<EventoAlmacenado>> _cajones = new();

    private readonly List<AggregateRoot> _iniciados   = new();
    private readonly List<AggregateRoot> _modificados = new();

    public void StartStream(AggregateRoot ar)
    {
        if (string.IsNullOrEmpty(ar.Id)) throw new InvalidOperationException("El agregado necesita un Id.");
        _iniciados.Add(ar);
    }

    public Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot, new()
    {
        var cajon = _cajones.GetValueOrDefault(id);
        if (cajon is null || cajon.Count == 0) return Task.FromResult<T?>(null);

        var ar = new T { Id = id };
        ar.Load(cajon.Select(s => s.EventData));
        _modificados.Add(ar);
        return Task.FromResult<T?>(ar);
    }

    // anexar un hecho suelto a un stream rastreado (parte del contrato IEventStore)
    public void AppendEvent(string id, object hecho)
    {
        var ar = _modificados.FirstOrDefault(a => a.Id == id) ?? _iniciados.FirstOrDefault(a => a.Id == id);
        if (ar is null) throw new InvalidOperationException($"El stream '{id}' no está rastreado en esta operación.");
        ar.AppendUncommitted(hecho);
    }

    public Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot =>
        Task.FromResult(_cajones.TryGetValue(id, out var c) && c.Count > 0);

    public Task SaveChangesAsync(CancellationToken ct = default)
    {
        foreach (var ar in _iniciados)
        {
            if (_cajones.TryGetValue(ar.Id, out var existe) && existe.Count > 0)
                throw new ConcurrencyException($"El stream '{ar.Id}' ya existe.");
            Volcar(ar, desdeVersion: ar.Version);
        }
        foreach (var ar in _modificados.Where(a => a.UncommittedEvents.Count > 0))
        {
            var actual = _cajones.TryGetValue(ar.Id, out var c) ? c.Count : 0;
            if (actual != ar.Version)
                throw new ConcurrencyException($"Esperaba la versión {ar.Version} de '{ar.Id}', pero está en {actual}.");
            Volcar(ar, desdeVersion: ar.Version);
        }
        _iniciados.Clear(); _modificados.Clear();
        return Task.CompletedTask;
    }

    private void Volcar(AggregateRoot ar, int desdeVersion)
    {
        var cajon = _cajones.GetValueOrDefault(ar.Id) ?? new();
        _cajones[ar.Id] = cajon;
        var version = desdeVersion;
        foreach (var hecho in ar.UncommittedEvents)
            cajon.Add(new EventoAlmacenado(++version, DateTime.UtcNow, hecho));
        ar.ClearUncommittedEvents();
    }
}

// ===================== Implementación 2: Marten (Postgres real) =====================
// El change-tracker (Unit of Work), igual que en «El almacén directo» pero como base reutilizable
public abstract class MartenUnitOfWork
{
    private readonly List<AggregateRoot> _modificados = [];
    private readonly List<AggregateRoot> _iniciados   = [];

    public IEnumerable<AggregateRoot> ModificadosConCambios =>
        _modificados.Where(ar => ar.UncommittedEvents.Count > 0);

    public void ClearChangeTracker() { _modificados.Clear(); _iniciados.Clear(); }
    protected void RastrearIniciado(AggregateRoot ar)   => _iniciados.Add(ar);
    protected void RastrearModificado(AggregateRoot? ar) { if (ar is not null) _modificados.Add(ar); }
}

// Marten da DOS sesiones (las registra e inyecta AddMarten): IDocumentSession (escribir) e IQuerySession (leer)
public class MartenEventStore(IDocumentSession session, IQuerySession querySession)
    : MartenUnitOfWork, IEventStore
{
    public void StartStream(AggregateRoot ar)                       // tu StartStream → Marten
    {
        if (string.IsNullOrEmpty(ar.Id)) throw new ArgumentException("El agregado necesita un Id.");
        session.Events.StartStream(ar.Id, ar.UncommittedEvents);
        RastrearIniciado(ar);
    }

    public async Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default)
        where T : AggregateRoot, new()                             // tu rehidratar → AggregateStreamAsync
    {
        var ar = await querySession.Events.AggregateStreamAsync<T>(id, token: ct);
        if (ar is not null) ar.Id = id;        // el id es la llave del stream: lo estampamos al cargar
        RastrearModificado(ar);
        return ar;
    }

    public void AppendEvent(string id, object hecho) => session.Events.Append(id, hecho);

    public async Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot
        => await querySession.Events.FetchStreamStateAsync(id, ct) is not null;

    public async Task SaveChangesAsync(CancellationToken ct = default)   // tu SaveChanges → session.SaveChangesAsync
    {
        foreach (var ar in ModificadosConCambios)
        {
            session.Events.Append(ar.Id, ar.UncommittedEvents);
            ar.ClearUncommittedEvents();
        }
        ClearChangeTracker();
        await session.SaveChangesAsync(ct);
    }
}

public class ConcurrencyException(string mensaje) : Exception(mensaje);

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public abstract class AggregateRoot
{
    private readonly List<object> _uncommittedEvents = new();

    public string Id { get; set; } = "";
    public int Version { get; protected set; }       // el agregado conoce su versión

    public IReadOnlyList<object> UncommittedEvents => _uncommittedEvents.AsReadOnly();
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();

    // permite al almacén anexar un hecho suelto (AppendEvent del contrato)
    public void AppendUncommitted(object hecho) => _uncommittedEvents.Add(hecho);

    protected void Raise(object hecho)
    {
        _uncommittedEvents.Add(hecho);   // lo recuerda
        Aplicar(hecho);                  // …y lo aplica al estado
    }

    public void Load(IEnumerable<object> historia)
    {
        foreach (var hecho in historia)
        {
            Aplicar(hecho);
            Version++;
        }
    }

    protected abstract void Aplicar(object hecho);
}

public class Empresa : AggregateRoot
{
    public string Nombre { get; private set; } = "";
    public string Plan   { get; private set; } = "";
    public bool   Suspendida    { get; private set; }
    public int    Reactivaciones { get; private set; }

    public Empresa() { }

    public static Empresa Registrar(string id, string nombre, string plan)
    {
        var empresa = new Empresa { Id = id };
        empresa.Raise(new EmpresaRegistrada(nombre, plan));
        return empresa;
    }

    public void CambiarPlan(string nuevoPlan)
    {
        if (Suspendida)
            throw new ReglaDeNegocioException("No se puede cambiar el plan de una empresa suspendida.");
        Raise(new PlanCambiado(nuevoPlan));
    }

    public void Suspender(string motivo)
    {
        if (Suspendida) return;
        Raise(new EmpresaSuspendida(motivo));
    }

    public void Reactivar() => Raise(new EmpresaReactivada());

    protected override void Aplicar(object hecho)
    {
        switch (hecho)
        {
            case EmpresaRegistrada e: Apply(e); break;
            case PlanCambiado e:      Apply(e); break;
            case EmpresaSuspendida e: Apply(e); break;
            case EmpresaReactivada e: Apply(e); break;
        }
    }

    public void Apply(EmpresaRegistrada e) { Nombre = e.Nombre; Plan = e.Plan; }
    public void Apply(PlanCambiado e)      => Plan = e.NuevoPlan;
    public void Apply(EmpresaSuspendida e) => Suspendida = true;
    public void Apply(EmpresaReactivada e) { Suspendida = false; Reactivaciones++; }
}

public class ReglaDeNegocioException(string mensaje) : Exception(mensaje);

public record RegistrarEmpresa(string EmpresaId, string Nombre, string Plan);
public record CambiarPlanDeEmpresa(string EmpresaId, string NuevoPlan);
public record SuspenderEmpresa(string EmpresaId, string Motivo);

public interface ICommandHandler<TCommand>
{
    Task HandleAsync(TCommand comando, CancellationToken ct = default);
}

public class RegistrarEmpresaHandler(IEventStore store) : ICommandHandler<RegistrarEmpresa>
{
    public async Task HandleAsync(RegistrarEmpresa cmd, CancellationToken ct = default)
    {
        if (await store.ExistsAsync<Empresa>(cmd.EmpresaId, ct))
            throw new InvalidOperationException($"La empresa {cmd.EmpresaId} ya existe.");
        var empresa = Empresa.Registrar(cmd.EmpresaId, cmd.Nombre, cmd.Plan);
        store.StartStream(empresa);
        await store.SaveChangesAsync(ct);
    }
}

public class CambiarPlanHandler(IEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public async Task HandleAsync(CambiarPlanDeEmpresa cmd, CancellationToken ct = default)
    {
        var empresa = await store.GetAggregateRootAsync<Empresa>(cmd.EmpresaId, ct)
                      ?? throw new InvalidOperationException($"No existe la empresa {cmd.EmpresaId}.");
        empresa.CambiarPlan(cmd.NuevoPlan);
        await store.SaveChangesAsync(ct);
    }
}

public class SuspenderHandler(IEventStore store) : ICommandHandler<SuspenderEmpresa>
{
    public async Task HandleAsync(SuspenderEmpresa cmd, CancellationToken ct = default)
    {
        var empresa = await store.GetAggregateRootAsync<Empresa>(cmd.EmpresaId, ct)
                      ?? throw new InvalidOperationException($"No existe la empresa {cmd.EmpresaId}.");
        empresa.Suspender(cmd.Motivo);
        await store.SaveChangesAsync(ct);
    }
}

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
