using AwesomeAssertions;

namespace Empresas.Historia.Tests;

public abstract class CommandHandlerTestBase
{
    protected string AggregateId = "emp-7";
    protected readonly TestStore EventStore = new();   // el de «El TestStore»

    protected void Given(params object[] eventos) => EventStore.AppendPreviousEvents(AggregateId, eventos);

    protected void Then(params object[] esperados) =>
        EventStore.GetNewEvents(AggregateId).Should().BeEquivalentTo(esperados);

    protected void And<T, TR>(Func<T, TR> proyeccion, TR esperado) where T : AggregateRoot, new() =>
        proyeccion(EventStore.GetAggregateRoot<T>(AggregateId)).Should().Be(esperado);
}

public abstract class CommandHandlerAsyncTest<TCommand> : CommandHandlerTestBase
{
    protected abstract ICommandHandler<TCommand> Handler { get; }   // por eso conservamos ICommandHandler<T> (Revelar Wolverine)
    protected async Task WhenAsync(TCommand cmd) { await Handler.HandleAsync(cmd); EventStore.SaveChanges(); }
}
