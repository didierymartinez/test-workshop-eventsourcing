using Marten;
using JasperFx.Events;
using Wolverine;
using Wolverine.Marten;
using Microsoft.Extensions.DependencyInjection;
using JasperFx;

// 🆕 De consola a app web: el host ahora es un WebApplicationBuilder (mismo contenedor de DI + servidor web).
var builder = WebApplication.CreateBuilder(args);

builder.UseWolverine(options =>
{
    options.Discovery.IncludeAssembly(typeof(CambiarPlanHandler).Assembly);  // descubre tus handlers
    options.UseRuntimeCompilation();                                          // genera el código del despacho

    options.Services.AddMarten(m =>
    {
        m.Connection("Host=localhost;Port=5432;Database=gestion_eventstore;Username=gestion;Password=dev_local_pwd");
        m.UseSystemTextJsonForSerialization();
        m.Events.StreamIdentity   = StreamIdentity.AsString;
        m.Events.EventNamingStyle = EventNamingStyle.SmarterTypeName;
    })
    .UseLightweightSessions()
    .IntegrateWithWolverine();                                                // Marten + outbox/inbox

    options.Policies.AddMiddleware<UnitOfWorkMiddleware>();                    // el middleware en cada comando
    options.Policies.AutoApplyTransactions();                                 // commit automático por mensaje
});

builder.Services.AddScoped<IEventStore, MartenEventStore>();   // el swap de «Revelar Marten» SIGUE

var app = builder.Build();

// 🔍 «Reflexión vs codegen»: la línea de comandos de Wolverine/JasperFx ENCHUFADA.
// Con args (p. ej. `codegen preview`/`codegen write`) corre ese comando; sin args, corre la demo.
if (args.Length > 0)
    return await app.RunJasperFxCommands(args);

await app.StartAsync();   // Wolverine necesita el host ARRANCADO antes de InvokeAsync (no basta Build())

// Despachamos comandos con IMessageBus.InvokeAsync — Wolverine resuelve el handler, corre el middleware y comitea.
await using (var scope = app.Services.CreateAsyncScope())
{
    var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
    await bus.InvokeAsync(new RegistrarEmpresa("emp-7", "Constructora Andes", "Básico"));
    await bus.InvokeAsync(new CambiarPlanDeEmpresa("emp-7", "Premium"));
    await bus.InvokeAsync(new SuspenderEmpresa("emp-7", "falta de pago"));
}

await using (var scope = app.Services.CreateAsyncScope())
{
    var store = scope.ServiceProvider.GetRequiredService<IEventStore>();
    var empresa = await store.GetAggregateRootAsync<Empresa>("emp-7");
    Console.WriteLine($"{empresa!.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}, versión {empresa.Version}");
}

await app.StopAsync();
return 0;

// ===================== El contrato =====================
public interface IEventStore
{
    void StartStream(AggregateRoot agregado);
    Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot, new();
    void AppendEvent(string id, object hecho);
    Task SaveChangesAsync(CancellationToken ct = default);
    Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot;
}

// ===================== Implementación 1: en memoria (sigue aquí; fuera de Wolverine) =====================
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
public abstract class MartenUnitOfWork
{
    private readonly List<AggregateRoot> _modificados = [];
    private readonly List<AggregateRoot> _iniciados   = [];

    public IEnumerable<AggregateRoot> ModificadosConCambios =>
        _modificados.Where(ar => ar.UncommittedEvents.Count > 0);

    // la unión de todo lo tocado (para limpiar al final)
    public IEnumerable<AggregateRoot> AggregateRoots => _iniciados.Union(_modificados);

    public void ClearChangeTracker() { _modificados.Clear(); _iniciados.Clear(); }
    protected void RastrearIniciado(AggregateRoot ar)   => _iniciados.Add(ar);
    protected void RastrearModificado(AggregateRoot? ar) { if (ar is not null) _modificados.Add(ar); }
}

public class MartenEventStore(IDocumentSession session, IQuerySession querySession)
    : MartenUnitOfWork, IEventStore
{
    public void StartStream(AggregateRoot ar)
    {
        if (string.IsNullOrEmpty(ar.Id)) throw new ArgumentException("El agregado necesita un Id.");
        session.Events.StartStream(ar.Id, ar.UncommittedEvents);
        RastrearIniciado(ar);
    }

    public async Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default)
        where T : AggregateRoot, new()
    {
        var ar = await querySession.Events.AggregateStreamAsync<T>(id, token: ct);
        if (ar is not null) ar.Id = id;
        RastrearModificado(ar);
        return ar;
    }

    public void AppendEvent(string id, object hecho) => session.Events.Append(id, hecho);

    // appendear una tanda de hechos a un stream (lo usa el middleware)
    public void AppendEvents(string id, IEnumerable<object> hechos) => session.Events.Append(id, hechos);

    public async Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot
        => await querySession.Events.FetchStreamStateAsync(id, ct) is not null;

    public async Task SaveChangesAsync(CancellationToken ct = default)   // queda para usos FUERA de Wolverine (tests, serverless)
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

// ===================== El middleware: prepara (no comitea; eso lo hace AutoApplyTransactions) =====================
public class UnitOfWorkMiddleware(IEventStore eventStore)
{
    // DESPUÉS de que el handler corrió bien: vuelca a la sesión los hechos pendientes
    public void After()
    {
        if (eventStore is not MartenEventStore m) return;
        foreach (var ar in m.ModificadosConCambios)
            m.AppendEvents(ar.Id, ar.UncommittedEvents);
    }

    // PASE LO QUE PASE: limpia los pendientes y el rastreo
    public void Finally()
    {
        if (eventStore is not MartenEventStore m) return;
        foreach (var ar in m.AggregateRoots) ar.ClearUncommittedEvents();
        m.ClearChangeTracker();
    }
}

public class ConcurrencyException(string mensaje) : Exception(mensaje);

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public abstract class AggregateRoot
{
    private readonly List<object> _uncommittedEvents = new();

    public string Id { get; set; } = "";
    public int Version { get; protected set; }

    public IReadOnlyList<object> UncommittedEvents => _uncommittedEvents.AsReadOnly();
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();
    public void AppendUncommitted(object hecho) => _uncommittedEvents.Add(hecho);

    protected void Raise(object hecho)
    {
        _uncommittedEvents.Add(hecho);
        Aplicar(hecho);
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

// Handlers: cargar/StartStream + decidir — SIN SaveChangesAsync (lo hace el middleware + AutoApplyTransactions)
public class RegistrarEmpresaHandler(IEventStore store) : ICommandHandler<RegistrarEmpresa>
{
    public async Task HandleAsync(RegistrarEmpresa cmd, CancellationToken ct = default)
    {
        if (await store.ExistsAsync<Empresa>(cmd.EmpresaId, ct))
            throw new InvalidOperationException($"La empresa {cmd.EmpresaId} ya existe.");
        var empresa = Empresa.Registrar(cmd.EmpresaId, cmd.Nombre, cmd.Plan);
        store.StartStream(empresa);                       // sin SaveChangesAsync
    }
}

public class CambiarPlanHandler(IEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public async Task HandleAsync(CambiarPlanDeEmpresa cmd, CancellationToken ct = default)
    {
        var empresa = await store.GetAggregateRootAsync<Empresa>(cmd.EmpresaId, ct)
                      ?? throw new InvalidOperationException($"No existe la empresa {cmd.EmpresaId}.");
        empresa.CambiarPlan(cmd.NuevoPlan);               // sin SaveChangesAsync
    }
}

public class SuspenderHandler(IEventStore store) : ICommandHandler<SuspenderEmpresa>
{
    public async Task HandleAsync(SuspenderEmpresa cmd, CancellationToken ct = default)
    {
        var empresa = await store.GetAggregateRootAsync<Empresa>(cmd.EmpresaId, ct)
                      ?? throw new InvalidOperationException($"No existe la empresa {cmd.EmpresaId}.");
        empresa.Suspender(cmd.Motivo);                    // sin SaveChangesAsync
    }
}

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
