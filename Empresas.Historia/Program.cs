// El tiempo de espera (async/await) — async de cabo a rabo

var store = new InMemoryEventStore();

var seed = store.AbrirStream<Empresa>("emp-7");
await seed.AppendAsync(new EmpresaRegistrada("Constructora Andes", "Básico"));

await new SuspenderHandler(store).HandleAsync(new SuspenderEmpresa("emp-7", "falta de pago"));

var andes = await store.AbrirStream<Empresa>("emp-7").GetAsync();
Console.WriteLine($"{andes.Id}: {andes.Nombre}, {(andes.Suspendida ? "suspendida" : "activa")}");


// ---- clases y records al final ----

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public class ConcurrencyException(string mensaje) : Exception(mensaje);

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
    public string Id { get; set; } = "";
    public void Load(IEnumerable<object> historia) { foreach (var h in historia) Aplicar(h); }
    protected abstract void Aplicar(object hecho);
}

public class Empresa : AggregateRoot
{
    public string Nombre { get; private set; } = "";
    public string Plan   { get; private set; } = "";
    public bool   Suspendida    { get; private set; }
    public int    Reactivaciones { get; private set; }

    public Empresa() { }

    public PlanCambiado CambiarPlan(string nuevoPlan)
    {
        if (Suspendida)
            throw new ReglaDeNegocioException("No se puede cambiar el plan de una empresa suspendida.");
        return new PlanCambiado(nuevoPlan);
    }

    public EmpresaSuspendida? Suspender(string motivo)
    {
        if (Suspendida)
            return null;
        return new EmpresaSuspendida(motivo);
    }

    public EmpresaReactivada Reactivar() => new();

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

public record CambiarPlanDeEmpresa(string EmpresaId, string NuevoPlan);
public record SuspenderEmpresa(string EmpresaId, string Motivo);

public interface ICommandHandler<TCommand>
{
    Task HandleAsync(TCommand comando, CancellationToken ct = default);
}

public class CambiarPlanHandler(InMemoryEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public async Task HandleAsync(CambiarPlanDeEmpresa cmd, CancellationToken ct = default)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);
        var empresa = await stream.GetAsync();
        await stream.AppendAsync(empresa.CambiarPlan(cmd.NuevoPlan));
    }
}

public class SuspenderHandler(InMemoryEventStore store) : ICommandHandler<SuspenderEmpresa>
{
    public async Task HandleAsync(SuspenderEmpresa cmd, CancellationToken ct = default)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);
        var empresa = await stream.GetAsync();             // 1. cargar (espera I/O)
        var hecho   = empresa.Suspender(cmd.Motivo);       // 2. actuar (CPU, instantáneo)
        if (hecho is not null) await stream.AppendAsync(hecho);  // 3. guardar (espera I/O)
    }
}

public class ReglaDeNegocioException(string mensaje) : Exception(mensaje);

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
