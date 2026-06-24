using Microsoft.Extensions.DependencyInjection;

// --- El contenedor de DI: arma el grafo por nosotros (sin new manual) ---
var services = new ServiceCollection();

services.AddSingleton<InMemoryEventStore>();   // UNO solo para toda la app
services.AddTransient<SuspenderHandler>();     // uno NUEVO cada vez que lo pidan

var proveedor = services.BuildServiceProvider();

// sembramos emp-7: como el almacén es Singleton, es la MISMA instancia que recibirá el handler
var almacen = proveedor.GetRequiredService<InMemoryEventStore>();
await almacen.AbrirStream<Empresa>("emp-7").AppendAsync(new EmpresaRegistrada("Constructora Andes", "Básico"));

// pedimos el handler: el contenedor lee su constructor, fabrica el InMemoryEventStore y se lo inyecta
var handler = proveedor.GetRequiredService<SuspenderHandler>();
await handler.HandleAsync(new SuspenderEmpresa("emp-7", "falta de pago"));

var andes = await almacen.AbrirStream<Empresa>("emp-7").GetAsync();
Console.WriteLine($"[DI] {andes.Id}: {andes.Nombre}, {(andes.Suspendida ? "suspendida" : "activa")}");

// --- El mini-contenedor de juguete: un contenedor no es magia ---
var c = new MiniContenedor();
var handlerMini = c.Resolver<SuspenderHandler>();   // lee su ctor, fabrica el InMemoryEventStore, lo inyecta
Console.WriteLine($"[MiniContenedor] resolvió: {handlerMini.GetType().Name}");

// Mini-contenedor: diccionario + reflexión del constructor + recursión.
public class MiniContenedor
{
    private readonly Dictionary<Type, Type> _registro = new();

    public void Registrar<TServicio, TImpl>() => _registro[typeof(TServicio)] = typeof(TImpl);

    public object Resolver(Type tipo)
    {
        var concreto = _registro.TryGetValue(tipo, out var impl) ? impl : tipo;

        var ctor = concreto.GetConstructors().First();
        var argumentos = ctor.GetParameters()
                             .Select(p => Resolver(p.ParameterType))   // recursión
                             .ToArray();

        return Activator.CreateInstance(concreto, argumentos)!;
    }

    public T Resolver<T>() => (T)Resolver(typeof(T));
}

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
        var hecho   = empresa.Suspender(cmd.Motivo);
        if (hecho is not null) await stream.AppendAsync(hecho);
    }
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

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
