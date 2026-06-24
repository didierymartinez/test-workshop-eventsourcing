// El almacén en memoria (Event Store) — un cajón por empresa, id como llave, concurrencia optimista

var store = new InMemoryEventStore();

var s7 = store.AbrirStream<Empresa>("emp-7");
s7.Append(new EmpresaRegistrada("Constructora Andes", "Básico"));
s7.Append(new PlanCambiado("Premium"));

var s9 = store.AbrirStream<Empresa>("emp-9");
s9.Append(new EmpresaRegistrada("Interprensa", "Básico"));

var andes = store.AbrirStream<Empresa>("emp-7").Get();
Console.WriteLine($"{andes.Id}: {andes.Nombre}, plan {andes.Plan}");
// emp-7: Constructora Andes, plan Premium


// ---- clases y records al final ----

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public class ConcurrencyException(string mensaje) : Exception(mensaje);

public class InMemoryEventStore
{
    private readonly Dictionary<string, List<EventoAlmacenado>> _cajones = new();

    public EventStream<T> AbrirStream<T>(string aggregateId) where T : AggregateRoot, new()
        => new(this, aggregateId);   // el almacén se incluye a sí mismo

    public IEnumerable<EventoAlmacenado> GetEvents(string aggregateId) =>
        _cajones.GetValueOrDefault(aggregateId) ?? Enumerable.Empty<EventoAlmacenado>();

    public void AppendEvent(string aggregateId, EventoAlmacenado evento)
    {
        var cajon = _cajones.GetValueOrDefault(aggregateId) ?? new();
        var versionActual = cajon.Count == 0 ? 0 : cajon[^1].Version;

        if (evento.Version != versionActual + 1)
            throw new ConcurrencyException(
                $"Esperaba la versión {versionActual + 1}, pero llegó la {evento.Version}. " +
                "Alguien escribió primero: recarga la empresa y reintenta.");

        _cajones[aggregateId] = cajon;
        cajon.Add(evento);
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

    public T Get()
    {
        var entidad = new T { Id = _aggregateId };
        var sobres  = _store.GetEvents(_aggregateId).ToList();
        entidad.Load(sobres.Select(s => s.EventData));
        _version = sobres.Count == 0 ? 0 : sobres[^1].Version;
        return entidad;
    }

    public void Append(object hecho)
    {
        _version++;
        _store.AppendEvent(_aggregateId, new(_version, DateTime.UtcNow, hecho));
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
    void Handle(TCommand comando);
}

public class CambiarPlanHandler(InMemoryEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public void Handle(CambiarPlanDeEmpresa cmd)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);
        var empresa = stream.Get();
        stream.Append(empresa.CambiarPlan(cmd.NuevoPlan));
    }
}

public class SuspenderHandler(InMemoryEventStore store) : ICommandHandler<SuspenderEmpresa>
{
    public void Handle(SuspenderEmpresa cmd)
    {
        var stream  = store.AbrirStream<Empresa>(cmd.EmpresaId);   // buscar por id
        var empresa = stream.Get();
        var hecho   = empresa.Suspender(cmd.Motivo);
        if (hecho is not null) stream.Append(hecho);
    }
}

public class ReglaDeNegocioException(string mensaje) : Exception(mensaje);

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
