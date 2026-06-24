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

// La empresa es dueña de su coherencia: se reconstruye leyendo su historia.
public class Empresa
{
    public string Nombre { get; private set; } = "";
    public string Plan   { get; private set; } = "";
    public bool   Suspendida    { get; private set; }
    public int    Reactivaciones { get; private set; }

    public Empresa(IEnumerable<object> historia)
    {
        foreach (var hecho in historia)
        {
            if (hecho is EmpresaRegistrada r) { Nombre = r.Nombre; Plan = r.Plan; }
            if (hecho is PlanCambiado p)      { Plan = p.NuevoPlan; }
            if (hecho is EmpresaSuspendida)   { Suspendida = true; }
            if (hecho is EmpresaReactivada)   { Suspendida = false; Reactivaciones++; }
        }
    }
}

// Los hechos: record inmutables (el pasado en piedra)
public record EmpresaRegistrada(string Nombre, string Plan);
public record PlanCambiado(string NuevoPlan);
public record EmpresaSuspendida(string Motivo);
public record EmpresaReactivada();
