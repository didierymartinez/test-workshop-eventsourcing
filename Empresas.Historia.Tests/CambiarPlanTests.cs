namespace Empresas.Historia.Tests;

public class CambiarPlanTests : CommandHandlerAsyncTest<CambiarPlanDeEmpresa>
{
    protected override ICommandHandler<CambiarPlanDeEmpresa> Handler => new CambiarPlanHandler(EventStore);

    [Fact]
    public async Task Cambiar_el_plan_de_una_empresa_activa_emite_PlanCambiado()
    {
        Given(new EmpresaRegistrada("Constructora Andes", "Básico"));   // historia previa
        await WhenAsync(new CambiarPlanDeEmpresa(AggregateId, "Premium"));  // el comando
        Then(new PlanCambiado("Premium"));                              // el hecho emitido
        And<Empresa, string>(e => e.Plan, "Premium");                   // …y el estado resultante
    }
}
