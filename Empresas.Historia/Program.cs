// El diario de Constructora Andes: una secuencia ordenada de hechos (un Stream).
var historia = new List<object>
{
    new EmpresaRegistrada("Constructora Andes", "Básico"),
    new PlanCambiado("Premium"),
    new EmpresaSuspendida("falta de pago"),
    new EmpresaReactivada(),
    new EmpresaSuspendida("incumplimiento de contrato"),
};

var empresa = new Empresa(historia);
Console.WriteLine($"{empresa.Nombre}: plan {empresa.Plan}, {(empresa.Suspendida ? "suspendida" : "activa")}, reactivada {empresa.Reactivaciones} vez/veces");

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

// La empresa: hereda el motor, aporta su propio Aplicar (switch por tipo).
public class Empresa : AggregateRoot
{
    public string Nombre { get; private set; } = "";
    public string Plan   { get; private set; } = "";
    public bool   Suspendida    { get; private set; }
    public int    Reactivaciones { get; private set; }

    public Empresa(IEnumerable<object> historia) => Load(historia);

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

// Los hechos: record inmutables (el pasado en piedra)
public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
