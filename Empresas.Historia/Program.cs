// 🔍 Comprueba — sección "El agregado que acumula":
// registrar → cambiar plan → suspender DOS veces (la 2ª es la prueba de idempotencia)
var store = new InMemoryEventStore();
await store.AbrirStream<Empresa>("emp-7").AppendAsync(new EmpresaRegistrada("Constructora Andes", "Básico"));

await new CambiarPlanHandler(store).HandleAsync(new CambiarPlanDeEmpresa("emp-7", "Premium"));
await new SuspenderHandler(store).HandleAsync(new SuspenderEmpresa("emp-7", "falta de pago"));
await new SuspenderHandler(store).HandleAsync(new SuspenderEmpresa("emp-7", "otra vez"));   // redundante

var empresa  = await store.AbrirStream<Empresa>("emp-7").GetAsync();
var historia = await store.GetEventsAsync("emp-7");
Console.WriteLine($"{empresa.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}; {historia.Count()} hechos");

public class InMemoryEventStore
{
    private readonly Dictionary<string, List<EventoAlmacenado>> _cajones = new();

    public EventStream<T> AbrirStream<T>(string aggregateId) where T : AggregateRoot, new()
        => new(this, aggregateId);

    public Task<IEnumerable<EventoAlmacenado>> GetEventsAsync(string aggregateId, CancellationToken ct = default) =>
        Task.FromResult<IEnumerable<EventoAlmacenado>>(
            _cajones.GetValueOrDefault(aggregateId) ?? Enumerable.Empty<EventoAlmacenado>());

    public Task AppendEventAsync(string aggregateId, EventoAlmacenado evento, CancellationToken ct = default)
    {
        var cajon = _cajones.GetValueOrDefault(aggregateId) ?? new();
        var versionActual = cajon.Count == 0 ? 0 : cajon[^1].Version;
        if (evento.Version != versionActual + 1)
            throw new ConcurrencyException(
                $"Esperaba la versión {versionActual + 1}, pero llegó la {evento.Version}.");

        _cajones[aggregateId] = cajon;
        cajon.Add(evento);
        return Task.CompletedTask;
    }
}

public class ConcurrencyException(string mensaje) : Exception(mensaje);

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public class EventStream<T> where T : AggregateRoot, new()
{
    private readonly InMemoryEventStore _store;
    private readonly string _aggregateId;
    private int _version;

    public EventStream(InMemoryEventStore store, string aggregateId)
    {
        _store = store;
        _aggregateId = aggregateId;
    }

    public async Task<T> GetAsync()
    {
        var entidad = new T { Id = _aggregateId };
        var sobres = (await _store.GetEventsAsync(_aggregateId)).ToList();
        entidad.Load(sobres.Select(s => s.EventData));
        _version = sobres.Count == 0 ? 0 : sobres[^1].Version;
        return entidad;
    }

    public async Task AppendAsync(object hecho)
    {
        _version++;
        await _store.AppendEventAsync(_aggregateId, new(_version, DateTime.UtcNow, hecho));
    }
}

public abstract class AggregateRoot
{
    private readonly List<object> _uncommittedEvents = new();

    public string Id { get; set; } = "";

    // los hechos que el agregado decidió pero aún no se han archivado
    public IReadOnlyList<object> UncommittedEvents => _uncommittedEvents.AsReadOnly();
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();

    // la empresa "levanta" un hecho: lo encola para que alguien lo persista
    protected void Raise(object hecho) => _uncommittedEvents.Add(hecho);

    public void Load(IEnumerable<object> historia)
    {
        foreach (var hecho in historia)
            Aplicar(hecho);
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

    public void CambiarPlan(string nuevoPlan)
    {
        if (Suspendida)   // validación: inválido → se RECHAZA (grita)
            throw new ReglaDeNegocioException("No se puede cambiar el plan de una empresa suspendida.");

        Raise(new PlanCambiado(nuevoPlan));
    }

    public void Suspender(string motivo)
    {
        if (Suspendida) return;   // idempotencia: redundante → NO-OP (calla, no encola)

        Raise(new EmpresaSuspendida(motivo));
    }

    public void Reactivar() => Raise(new EmpresaReactivada());

    protected override void Aplicar(object hecho)
    {
        switch (hecho)
        {
            case EmpresaRegistrada r: Nombre = r.Nombre; Plan = r.Plan; break;
            case PlanCambiado p:      Plan = p.NuevoPlan;                break;
            case EmpresaSuspendida:   Suspendida = true;                break;
            case EmpresaReactivada:   Suspendida = false; Reactivaciones++; break;
        }
    }
}

public class ReglaDeNegocioException(string mensaje) : Exception(mensaje);

public record CambiarPlanDeEmpresa(string EmpresaId, string NuevoPlan);
public record SuspenderEmpresa(string EmpresaId, string Motivo);

public interface ICommandHandler<TCommand>
{
    Task HandleAsync(TCommand comando, CancellationToken ct = default);
}

public class SuspenderHandler(InMemoryEventStore store) : ICommandHandler<SuspenderEmpresa>
{
    public async Task HandleAsync(SuspenderEmpresa cmd, CancellationToken ct = default)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);
        var empresa = await stream.GetAsync();

        empresa.Suspender(cmd.Motivo);                      // si es redundante, no encola nada

        foreach (var hecho in empresa.UncommittedEvents)    // … y el foreach no archiva nada
            await stream.AppendAsync(hecho);
        empresa.ClearUncommittedEvents();
    }
}

public class CambiarPlanHandler(InMemoryEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public async Task HandleAsync(CambiarPlanDeEmpresa cmd, CancellationToken ct = default)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);
        var empresa = await stream.GetAsync();

        empresa.CambiarPlan(cmd.NuevoPlan);                 // decide → encola

        foreach (var hecho in empresa.UncommittedEvents)    // drena lo que haya
            await stream.AppendAsync(hecho);
        empresa.ClearUncommittedEvents();
    }
}

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
