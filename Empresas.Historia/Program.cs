// El flujo de vida (EventStream) — envolvemos la historia en un EventStream<T>

var stream = new EventStream<Empresa>();
stream.Append(new EmpresaRegistrada("Constructora Andes", "Básico"));   // anota
stream.Append(new PlanCambiado("Premium"));                            // anota

var empresa = stream.Get();   // lee: instancia y rehidrata
Console.WriteLine($"{empresa.Nombre}: plan {empresa.Plan}");


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

    public Empresa() { }   // sin parámetros: el envoltorio la crea vacía y la rehidrata

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

public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
