// Decidir el futuro — la empresa emite sus propios hechos (cargar -> actuar -> guardar)

var stream = new EventStream<Empresa>();
stream.Append(new EmpresaRegistrada("Constructora Andes", "Básico"));   // su historia previa

var empresa = stream.Get();                       // 1. CARGAR (rehidratar)
Console.WriteLine($"[antes] plan {empresa.Plan}");

var hecho = empresa.CambiarPlan("Enterprise");    // 2. ACTUAR (la empresa decide y emite el hecho)
stream.Append(hecho);                             // 3. GUARDAR (el stream lo archiva)

var verificacion = stream.Get();                  // recargamos del mismo stream
Console.WriteLine($"[después] plan {verificacion.Plan}");


// ---- clases y records al final ----

public class EventStream<T> where T : AggregateRoot, new()
{
    private readonly List<object> _historia = new();

    public void Append(object hecho) => _historia.Add(hecho);

    public T Get()
    {
        var entidad = new T();
        entidad.Load(_historia);
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
        // (a) VALIDACIÓN — la operación es inválida → se RECHAZA (es un error)
        if (Suspendida)
            throw new ReglaDeNegocioException("No se puede cambiar el plan de una empresa suspendida.");

        return new PlanCambiado(nuevoPlan);
    }

    public EmpresaSuspendida? Suspender(string motivo)
    {
        // (b) IDEMPOTENCIA — operación válida pero redundante → NO-OP (no es un error)
        if (Suspendida)
            return null;   // ya está suspendida: no emitimos un hecho duplicado

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
