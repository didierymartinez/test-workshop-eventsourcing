// 🔍 Comprueba — sección "El almacén directo (y la Unit of Work)":
// alta con StartStream → 2 mutaciones cargar→decidir→guardar → rehidratar, sin EventStream ni foreach de drenado.
var store = new InMemoryEventStore();

await new RegistrarEmpresaHandler(store).HandleAsync(new RegistrarEmpresa("emp-7", "Constructora Andes", "Básico"));
await new CambiarPlanHandler(store).HandleAsync(new CambiarPlanDeEmpresa("emp-7", "Premium"));
await new SuspenderHandler(store).HandleAsync(new SuspenderEmpresa("emp-7", "falta de pago"));

var empresa = await store.GetAggregateRootAsync<Empresa>("emp-7");
Console.WriteLine($"{empresa!.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}, versión {empresa.Version}");

public class InMemoryEventStore
{
    private readonly Dictionary<string, List<EventoAlmacenado>> _cajones = new();

    // La Unit of Work: recuerda qué tocaste en esta operación
    private readonly List<AggregateRoot> _iniciados   = new();   // nacieron aquí (StartStream)
    private readonly List<AggregateRoot> _modificados = new();   // se cargaron para mutar (Get)

    // ALTA: un agregado nuevo abre su stream
    public void StartStream(AggregateRoot ar)
    {
        if (string.IsNullOrEmpty(ar.Id)) throw new InvalidOperationException("El agregado necesita un Id.");
        _iniciados.Add(ar);                       // se persiste en SaveChangesAsync, no ya
    }

    // CARGA: rehidrata Y lo apunta como "tocado"
    public Task<T?> GetAggregateRootAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot, new()
    {
        var cajon = _cajones.GetValueOrDefault(id);
        if (cajon is null || cajon.Count == 0) return Task.FromResult<T?>(null);

        var ar = new T { Id = id };
        ar.Load(cajon.Select(s => s.EventData));
        _modificados.Add(ar);                     // lo rastreo: al guardar, drenaré sus hechos
        return Task.FromResult<T?>(ar);
    }

    public Task<bool> ExistsAsync<T>(string id, CancellationToken ct = default) where T : AggregateRoot =>
        Task.FromResult(_cajones.TryGetValue(id, out var c) && c.Count > 0);

    // GUARDA: drena los hechos pendientes de todo lo rastreado, en un solo paso
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
            if (actual != ar.Version)             // concurrencia optimista, ahora con ar.Version
                throw new ConcurrencyException($"Esperaba la versión {ar.Version} de '{ar.Id}', pero está en {actual}.");
            Volcar(ar, desdeVersion: ar.Version);
        }
        _iniciados.Clear(); _modificados.Clear();   // limpia el rastreo: la operación terminó
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

public class ConcurrencyException(string mensaje) : Exception(mensaje);

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public abstract class AggregateRoot
{
    private readonly List<object> _uncommittedEvents = new();

    public string Id { get; set; } = "";
    public int Version { get; protected set; }       // el agregado conoce su versión

    // los hechos que el agregado decidió pero aún no se han archivado
    public IReadOnlyList<object> UncommittedEvents => _uncommittedEvents.AsReadOnly();
    public void ClearUncommittedEvents() => _uncommittedEvents.Clear();

    // la empresa "levanta" un hecho: lo encola para que alguien lo persista
    protected void Raise(object hecho)
    {
        _uncommittedEvents.Add(hecho);   // lo recuerda (como en «El agregado que acumula»)
        Aplicar(hecho);                  // …y ahora también lo aplica al estado
    }

    public void Load(IEnumerable<object> historia)
    {
        foreach (var hecho in historia)
        {
            Aplicar(hecho);
            Version++;                                // cada hecho persistido sube la versión cargada
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

    // ALTA: el evento de creación NO lleva id; el id es la llave del stream
    public static Empresa Registrar(string id, string nombre, string plan)
    {
        var empresa = new Empresa { Id = id };
        empresa.Raise(new EmpresaRegistrada(nombre, plan));
        return empresa;
    }

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

    // el dispatcher: el switch ya solo ENRUTA al Apply tipado (sigue siendo el override de la base)
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

    // un Apply por tipo: aquí vive la mutación de estado (público: la herramienta lo llamará por convención)
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

public class RegistrarEmpresaHandler(InMemoryEventStore store) : ICommandHandler<RegistrarEmpresa>
{
    public async Task HandleAsync(RegistrarEmpresa cmd, CancellationToken ct = default)
    {
        if (await store.ExistsAsync<Empresa>(cmd.EmpresaId, ct))   // ya tiene stream → no se registra dos veces
            throw new InvalidOperationException($"La empresa {cmd.EmpresaId} ya existe.");
        var empresa = Empresa.Registrar(cmd.EmpresaId, cmd.Nombre, cmd.Plan);
        store.StartStream(empresa);
        await store.SaveChangesAsync(ct);
    }
}

public class CambiarPlanHandler(InMemoryEventStore store) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public async Task HandleAsync(CambiarPlanDeEmpresa cmd, CancellationToken ct = default)
    {
        var empresa = await store.GetAggregateRootAsync<Empresa>(cmd.EmpresaId, ct)
                      ?? throw new InvalidOperationException($"No existe la empresa {cmd.EmpresaId}.");
        empresa.CambiarPlan(cmd.NuevoPlan);
        await store.SaveChangesAsync(ct);
    }
}

public class SuspenderHandler(InMemoryEventStore store) : ICommandHandler<SuspenderEmpresa>
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
