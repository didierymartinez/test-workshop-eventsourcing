// El despachador — tabla tipo-de-comando -> handler, sin switch ni clases concretas

var stream = new EventStream<Empresa>();
stream.Append(new EmpresaRegistrada("Constructora Andes", "Básico"));

var despachador = new Despachador();
despachador.Registrar(new CambiarPlanHandler(stream));
despachador.Registrar(new SuspenderHandler(stream));

object comando = new SuspenderEmpresa("falta de pago");   // llega como object, sin más
despachador.Enviar(comando);                              // encuentra SuspenderHandler por el tipo y lo llama

Console.WriteLine(stream.Get().Suspendida ? "suspendida" : "activa");   // suspendida


// ---- clases y records al final ----

public class Despachador
{
    private readonly Dictionary<Type, Action<object>> _handlers = new();

    public void Registrar<T>(ICommandHandler<T> handler)
        => _handlers[typeof(T)] = (object comando) => handler.Handle((T)comando);

    public void Enviar(object comando)
        => _handlers[comando.GetType()](comando);
}

public record EventoAlmacenado(int Version, DateTime Timestamp, object EventData);

public class EventStream<T> where T : AggregateRoot, new()
{
    private readonly List<EventoAlmacenado> _historia = new();
    private int _version;

    public void Append(object hecho)
        => _historia.Add(new EventoAlmacenado(++_version, DateTime.UtcNow, hecho));

    public T Get()
    {
        var entidad = new T();
        entidad.Load(_historia.Select(s => s.EventData));
        return entidad;
    }
}

public abstract class AggregateRoot
{
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

public record CambiarPlanDeEmpresa(string NuevoPlan);
public record SuspenderEmpresa(string Motivo);

public interface ICommandHandler<TCommand>
{
    void Handle(TCommand comando);
}

public class CambiarPlanHandler(EventStream<Empresa> stream) : ICommandHandler<CambiarPlanDeEmpresa>
{
    public void Handle(CambiarPlanDeEmpresa cmd) =>
        stream.Append(stream.Get().CambiarPlan(cmd.NuevoPlan));
}

public class SuspenderHandler(EventStream<Empresa> stream) : ICommandHandler<SuspenderEmpresa>
{
    public void Handle(SuspenderEmpresa cmd)
    {
        var hecho = stream.Get().Suspender(cmd.Motivo);
        if (hecho is not null) stream.Append(hecho);
    }
}

public class ReglaDeNegocioException(string mensaje) : Exception(mensaje);

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
