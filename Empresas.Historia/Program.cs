// El handler orquesta cargar -> actuar -> guardar; el Program solo pide la operación.
var stream = new EventStream<Empresa>();
stream.Append(new EmpresaRegistrada("Constructora Andes", "Básico"));

var handlerCambiar = new CambiarPlanHandler(stream);
handlerCambiar.Handle("Premium");

var handlerSuspender = new SuspenderHandler(stream);
handlerSuspender.Handle("falta de pago");

Console.WriteLine(stream.Get().Suspendida ? "suspendida" : "activa");   // suspendida

// Un handler por comando (responsabilidad única): cargar -> decidir -> guardar.
public class CambiarPlanHandler(EventStream<Empresa> stream)
{
    public void Handle(string nuevoPlan) =>
        stream.Append(stream.Get().CambiarPlan(nuevoPlan));
}

public class SuspenderHandler(EventStream<Empresa> stream)
{
    public void Handle(string motivo)
    {
        var hecho = stream.Get().Suspender(motivo);
        if (hecho is not null) stream.Append(hecho);
    }
}

// El sobre: envuelve el hecho con su POSICIÓN (versión) y cuándo se anotó.
public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

// El envoltorio genérico: ahora numera cada hecho en un sobre.
public class EventStream<T> where T : AggregateRoot, new()
{
    private readonly List<EventoAlmacenado> _historia = new();
    private int _version;

    public void Append(object hecho)
        => _historia.Add(new EventoAlmacenado(++_version, DateTime.UtcNow, hecho));

    public T Get()
    {
        var entidad = new T();
        entidad.Load(_historia.Select(s => s.EventData));   // solo el hecho, no el sobre
        return entidad;
    }
}

// El motor de replay: una sola vez, en la base.
public abstract class AggregateRoot
{
    public void Load(IEnumerable<object> historia)
    {
        foreach (var hecho in historia)
            Aplicar(hecho);
    }

    protected abstract void Aplicar(object hecho);
}

// La empresa: decide (emite hechos protegiendo reglas) y aplica (en el replay).
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

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
