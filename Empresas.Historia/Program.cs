using Marten;
using JasperFx.Events;
using Wolverine;
using Wolverine.Marten;
using Wolverine.RabbitMQ;
using Marten.Events.Aggregation;
using JasperFx.Events.Projections;
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
        m.Projections.Add<EmpresaResumenProjection>(ProjectionLifecycle.Inline);   // §25 CQRS: proyección inline

        // ===== §27 Multi-tenancy: la llave real es (tenant, id) =====
        m.Events.TenancyStyle = JasperFx.MultiTenancy.TenancyStyle.Conjoined;   // tablas compartidas + columna tenant_id
        m.Policies.AllDocumentsAreMultiTenanted();                        // todo documento lleva su tenant
    })
    .UseLightweightSessions()
    .IntegrateWithWolverine();                                                // Marten + outbox/inbox

    options.Policies.AddMiddleware<UnitOfWorkMiddleware>();                    // el middleware en cada comando
    options.Policies.AutoApplyTransactions();                                 // commit automático por mensaje

    options.Policies.UseDurableOutboxOnAllSendingEndpoints();   // §23 outbox para todo lo que sale
    options.Policies.UseDurableInboxOnAllListeners();           // §23 inbox para todo lo que entra

    // ===== §24 Transportes: RabbitMQ real =====
    // 1) conectar a RabbitMQ y dejar que Wolverine cree exchanges/colas
    options.UseRabbitMqUsingNamedConnection("rabbitmq").AutoProvision();

    // 2) publicar TODOS tus IPublicEvent a un exchange con el nombre de tu servicio (outbox durable)
    var contratos = typeof(EmpresaSuspendida).Assembly;   // el ensamblado donde viven los contratos/eventos
    foreach (var tipo in contratos.GetTypes().Where(t => t.IsAssignableTo(typeof(IPublicEvent))))
        options.PublishMessage(tipo).ToRabbitExchange("gestion-empresas").UseDurableOutbox();

    // 3) para verlo dar la vuelta SOLO: ato la cola a mi PROPIO exchange (inbox durable)
    options.ListenToRabbitQueue("gestion-empresas.cola", c => c.BindExchange("gestion-empresas")).UseDurableInbox();
});

builder.Services.AddScoped<IEventStore, MartenEventStore>();   // el swap de «Revelar Marten» SIGUE

// ===== §28 Resolver el tenant: no se pasa por parámetro, se resuelve del contexto =====
builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<ITenantResolver, ProxyTenantResolver>();   // §29 híbrido: HTTP o sobre del mensaje

var app = builder.Build();

// 🔍 «Reflexión vs codegen»: la línea de comandos de Wolverine/JasperFx ENCHUFADA.
// Con args (p. ej. `codegen preview`/`codegen write`) corre ese comando; sin args, corre la demo.
if (args.Length > 0)
    return await app.RunJasperFxCommands(args);

await app.StartAsync();   // Wolverine necesita el host ARRANCADO antes de InvokeAsync (no basta Build())

// ===================== §30 TestStore: rehidrata SIN base de datos (reflexión) =====================
{
    var store = new TestStore();
    store.AppendPreviousEvents("emp-7",
        new EmpresaRegistrada("Constructora Andes", "Básico"),
        new EmpresaSuspendida("falta de pago"));

    var emp = store.GetAggregateRoot<Empresa>("emp-7");
    Console.WriteLine($"[§30] {emp!.Nombre}, plan {emp.Plan}, suspendida={emp.Suspendida} (sin Postgres)");
}

// ===================== §27 Multi-tenancy: dos tenants, el MISMO id, no se pisan =====================
{
    var docStore = app.Services.GetRequiredService<IDocumentStore>();

    // sinco/emp-7 y acme/emp-7: mismo id, tenants distintos -> sesión POR TENANT (no por parámetro)
    // (idempotente para poder re-correr la demo: solo arranca el stream si no existe en ese tenant)
    await using (var s = docStore.LightweightSession("sinco"))
    {
        if (await s.Events.FetchStreamStateAsync("emp-7") is null)
            s.Events.StartStream<Empresa>("emp-7", new EmpresaRegistrada("Constructora Andes", "Básico"));
        await s.SaveChangesAsync();
    }
    await using (var s = docStore.LightweightSession("acme"))
    {
        if (await s.Events.FetchStreamStateAsync("emp-7") is null)
            s.Events.StartStream<Empresa>("emp-7", new EmpresaRegistrada("Otra Empresa SA", "Premium"));
        await s.SaveChangesAsync();
    }

    await using (var qs = docStore.QuerySession("sinco"))
    {
        var emp = await qs.Events.AggregateStreamAsync<Empresa>("emp-7");
        Console.WriteLine($"[§27] sinco/emp-7 -> {emp!.Nombre}, plan {emp.Plan}");
    }
    await using (var qa = docStore.QuerySession("acme"))
    {
        var emp = await qa.Events.AggregateStreamAsync<Empresa>("emp-7");
        Console.WriteLine($"[§27] acme/emp-7  -> {emp!.Nombre}, plan {emp.Plan}");
    }
}

// 🔍 Comprueba (§23 outbox/inbox): el relay no duplica
{
    var almacen = new AlmacenConOutbox();
    var bus = new BusEnMemoria();
    var inbox = new Inbox();

    almacen.GuardarConOutbox(new EmpresaSuspendida("falta de pago"));
    Console.WriteLine($"pendientes en outbox: {almacen.Pendientes}");   // 1

    almacen.DrenarOutbox(bus, inbox);
    almacen.DrenarOutbox(bus, inbox);    // segundo drenado: nada nuevo
    Console.WriteLine($"publicados: {bus.Publicados}, pendientes: {almacen.Pendientes}");   // 1, 0
}

// Despachamos comandos con IMessageBus.InvokeAsync — Wolverine resuelve el handler, corre el middleware y comitea.
await using (var scope = app.Services.CreateAsyncScope())
{
    var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
    await bus.InvokeAsync(new RegistrarEmpresa("emp-7", "Constructora Andes", "Básico"));
    await bus.InvokeAsync(new CambiarPlanDeEmpresa("emp-7", "Premium"));
    await bus.InvokeAsync(new SuspenderEmpresa("emp-7", "falta de pago"));

    // §24 Reto: publicar un IPublicEvent al broker (ejercita la ruta ToRabbitExchange + outbox durable)
    await bus.PublishAsync(new EmpresaSuspendida("falta de pago"));

    // §29: el tenant viaja CON el mensaje (DeliveryOptions.TenantId) + el user_id como header del sobre
    await bus.PublishAsync(new ProbarTenantPorMensaje("hola"),
        new DeliveryOptions { TenantId = "acme" }.WithHeader("user_id", "u-1"));
}

await using (var scope = app.Services.CreateAsyncScope())
{
    var store = scope.ServiceProvider.GetRequiredService<IEventStore>();
    var empresa = await store.GetAggregateRootAsync<Empresa>("emp-7");
    Console.WriteLine($"{empresa!.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}, versión {empresa.Version}");
}

// 🔍 §25 CQRS: consultar la proyección (modelo de lectura) SIN reproducir streams
await using (var scope = app.Services.CreateAsyncScope())
{
    var store = scope.ServiceProvider.GetRequiredService<IDocumentStore>();
    await using var querySession = store.QuerySession();

    var suspendidas = await querySession.Query<EmpresaResumen>()
        .Where(e => e.Suspendida)
        .ToListAsync(CancellationToken.None);

    Console.WriteLine($"[§25] empresas suspendidas (proyección): {suspendidas.Count}");
    foreach (var r in suspendidas)
        Console.WriteLine($"[§25]   - {r.Id}: {r.Nombre}, plan {r.Plan}, suspendida={r.Suspendida}");
}

Console.WriteLine("[§24] esperando a que el relay durable drene el outbox a RabbitMQ...");
await Task.Delay(5000);   // §24 dejar respirar al relay durable (outbox -> RabbitMQ)

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

// ===================== §30 TestStore: IEventStore en memoria para tests (rehidrata por reflexión) =====================
public class TestStore : IEventStore
{
    private readonly Dictionary<string, List<object>> _previos = new();   // historia sembrada (Given)
    private readonly Dictionary<string, List<object>> _nuevos  = new();   // hechos producidos (Then)
    private readonly List<AggregateRoot> _rastreados = new();             // lo cargado/iniciado en esta operación
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(Type, Type), System.Reflection.MethodInfo?> _cache = new();

    public void AppendPreviousEvents(string id, params object[] eventos) =>          // Given
        (_previos[id] = _previos.GetValueOrDefault(id) ?? new()).AddRange(eventos);

    public IEnumerable<object> GetNewEvents(string id) => _nuevos.GetValueOrDefault(id) ?? [];   // Then

    // --- IEventStore: lo que el handler usa (carga + rastrea; guardar drena a _nuevos) ---
    public void StartStream(AggregateRoot ar) => _rastreados.Add(ar);

    public Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot, new()
    {
        var ar = new T { Id = id };
        if (_previos.TryGetValue(id, out var historia)) foreach (var e in historia) ApplyEvent(ar, e);   // rehidrata
        _rastreados.Add(ar);                                                                             // rastrea
        return Task.FromResult<T?>(ar);
    }

    public void AppendEvent(string id, object e) => (_nuevos[id] = _nuevos.GetValueOrDefault(id) ?? new()).Add(e);
    public Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot
        => Task.FromResult(_previos.ContainsKey(id));
    public Task SaveChangesAsync(CancellationToken ct = default) { SaveChanges(); return Task.CompletedTask; }

    // el "commit" del test: vuelca los hechos nuevos de lo rastreado a _nuevos (lo que Then observará)
    public void SaveChanges()
    {
        foreach (var ar in _rastreados)
        {
            (_nuevos[ar.Id] = _nuevos.GetValueOrDefault(ar.Id) ?? new()).AddRange(ar.UncommittedEvents);
            ar.ClearUncommittedEvents();
        }
        _rastreados.Clear();
    }

    // rehidrata para inspección (historia previa + hechos nuevos), aplicando por reflexión
    public T GetAggregateRoot<T>(string id) where T : AggregateRoot, new()
    {
        var ar = new T { Id = id };
        if (_previos.TryGetValue(id, out var p)) foreach (var e in p) ApplyEvent(ar, e);
        if (_nuevos.TryGetValue(id, out var n)) foreach (var e in n) ApplyEvent(ar, e);
        return ar;
    }

    // reflexión: halla el Apply(TipoConcreto) del agregado y lo invoca (cacheado)
    private static void ApplyEvent<T>(T ar, object evento)
    {
        var metodo = _cache.GetOrAdd((typeof(T), evento.GetType()),
            llave => llave.Item1.GetMethod("Apply", new[] { llave.Item2 }));
        metodo?.Invoke(ar, new[] { evento });
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

public interface IEvent;                          // marcador raíz: interfaz VACÍA
public interface IPublicEvent  : IEvent;          // cruza la frontera (EDA)
public interface IPrivateEvent : IEvent;          // interno al servicio

// ===================== Senders y el sobre (§22) =====================
public interface IPublicEventSender
{
    Task PublishAsync(params IPublicEvent[] eventos);
    Task PublishAsync(string groupId, params IPublicEvent[] eventos);   // groupId = orden FIFO por grupo
}
public interface IPrivateEventSender
{
    Task PublishAsync(params IPrivateEvent[] eventos);
    Task PublishAsync(string groupId, params IPrivateEvent[] eventos);
}

// en memoria: acumula lo enviado (para inspeccionar / para tests)
public class TestPublicEventSender : IPublicEventSender
{
    private readonly List<IPublicEvent> _enviados = new();
    public IReadOnlyList<IPublicEvent> Enviados => _enviados.AsReadOnly();
    public Task PublishAsync(params IPublicEvent[] eventos)               { _enviados.AddRange(eventos); return Task.CompletedTask; }
    public Task PublishAsync(string groupId, params IPublicEvent[] eventos) { _enviados.AddRange(eventos); return Task.CompletedTask; }
}

public record Sobre(object Payload, string TenantId, string? UserId = null, string? GroupId = null);

// ===================== §28 Resolver el tenant =====================
public interface ITenantResolver
{
    string TenantId { get; }
    string UserId { get; }
}

// lee de headers HTTP que un gateway de confianza ya estampó
public class TrustedHeadersTenantResolver(IHttpContextAccessor accessor) : ITenantResolver
{
    public string TenantId => Leer("X-Tenant-Id");
    public string UserId   => Leer("X-User-Id");

    private string Leer(string header)
    {
        var valor = accessor.HttpContext?.Request.Headers[header].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(valor))
            throw new InvalidOperationException($"Falta el header de contexto '{header}'.");
        return valor;
    }
}

// ===================== §29 El tenant viaja con el mensaje =====================
// resolver para handlers del daemon: lee del sobre del mensaje (lo propaga Wolverine)
public class WolverineMessageContextTenantResolver(IMessageContext messageContext) : ITenantResolver
{
    public string TenantId => messageContext.TenantId
        ?? throw new InvalidOperationException("El mensaje no se publicó con DeliveryOptions.TenantId.");
    public string UserId => messageContext.Envelope?.Headers.GetValueOrDefault("user_id")
        ?? throw new InvalidOperationException("El mensaje no trae el header user_id.");
}

// proxy: una sola ITenantResolver, dos fuentes — elige por presencia de HttpContext
public class ProxyTenantResolver(IMessageContext messageContext, IHttpContextAccessor accessor) : ITenantResolver
{
    private readonly ITenantResolver _real = accessor.HttpContext is not null
        ? new TrustedHeadersTenantResolver(accessor)                 // hay request HTTP → headers
        : new WolverineMessageContextTenantResolver(messageContext); // no → el sobre del mensaje
    public string TenantId => _real.TenantId;
    public string UserId   => _real.UserId;
}

// ===================== Outbox / Inbox de juguete (§23) =====================
public class AlmacenConOutbox
{
    private readonly List<object> _eventos = new();
    private readonly List<(string Id, object Msg)> _outbox = new();
    public int Pendientes => _outbox.Count;           // cuántos mensajes esperan en la bandeja

    public void GuardarConOutbox(object hecho)        // el hecho y su mensaje, en la MISMA operación
    {
        _eventos.Add(hecho);
        _outbox.Add((Guid.NewGuid().ToString(), hecho));
    }

    public void DrenarOutbox(BusEnMemoria bus, Inbox inbox)   // el relay: publica y vacía
    {
        foreach (var (id, msg) in _outbox.ToList())
        {
            if (!inbox.YaProcesado(id)) bus.Publicar(id, msg);   // exactly-once de PROCESAMIENTO: el inbox evita reprocesar
            _outbox.RemoveAll(p => p.Id == id);
        }
    }
}

public class Inbox    // idempotencia del consumidor: ids ya vistos
{
    private readonly HashSet<string> _procesados = new();
    public bool YaProcesado(string id) => !_procesados.Add(id);
}

// el "broker" de juguete: en producción es RabbitMQ / Azure Service Bus
public class BusEnMemoria
{
    public int Publicados { get; private set; }
    public void Publicar(string id, object msg) => Publicados++;
}

public abstract class AggregateRoot
{
    private readonly List<object> _uncommittedEvents = new();

    public string Id { get; set; } = "";
    public int Version { get; protected set; }

    public IReadOnlyList<object> UncommittedEvents => _uncommittedEvents.AsReadOnly();
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();
    public void AppendUncommitted(object hecho) => _uncommittedEvents.Add(hecho);

    public IPublicEvent[]  GetPublicEvents()  => _uncommittedEvents.OfType<IPublicEvent>().ToArray();
    public IPrivateEvent[] GetPrivateEvents() => _uncommittedEvents.OfType<IPrivateEvent>().ToArray();

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
public record EmpresaSuspendida(string Motivo) : IPublicEvent;
public record EmpresaReactivada()              : IPublicEvent;

// ===================== §29 verificación e2e: el tenant viaja en el sobre hasta el daemon =====================
public record ProbarTenantPorMensaje(string Que);

public class ProbarTenantPorMensajeHandler
{
    // Wolverine inyecta ITenantResolver; SIN HttpContext (corre en el worker), el proxy
    // cae al WolverineMessageContextTenantResolver y lee el TenantId/user_id DEL SOBRE.
    public void Handle(ProbarTenantPorMensaje msg, ITenantResolver tenant)
    {
        Console.WriteLine($"[§29] handler del daemon resolvió → tenant '{tenant.TenantId}', user '{tenant.UserId}' (sin HttpContext)");
    }
}

// ===================== §25 CQRS: modelo de lectura + proyección =====================
public class EmpresaResumen
{
    public string Id { get; set; } = "";
    public string Nombre { get; set; } = "";
    public string Plan { get; set; } = "";
    public bool   Suspendida { get; set; }
}

public partial class EmpresaResumenProjection : SingleStreamProjection<EmpresaResumen, string>
{
    public void Apply(EmpresaRegistrada e, EmpresaResumen r) { r.Nombre = e.Nombre; r.Plan = e.Plan; }
    public void Apply(PlanCambiado e, EmpresaResumen r)      => r.Plan = e.NuevoPlan;
    public void Apply(EmpresaSuspendida e, EmpresaResumen r) => r.Suspendida = true;
    public void Apply(EmpresaReactivada e, EmpresaResumen r) => r.Suspendida = false;
}
