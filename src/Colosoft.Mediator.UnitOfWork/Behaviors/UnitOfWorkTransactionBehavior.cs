using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Colosoft.Mediator.Behaviors
{
    public class UnitOfWorkTransactionBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
        where TRequest : IRequest<TResponse>
    {
        private readonly IUnitOfWorkProvider unitOfWorkProvider;
        private readonly ILogger<UnitOfWorkTransactionBehavior<TRequest, TResponse>> logger;

        public UnitOfWorkTransactionBehavior(
            IUnitOfWorkProvider unitOfWorkProvider,
            ILogger<UnitOfWorkTransactionBehavior<TRequest, TResponse>> logger)
        {
            this.unitOfWorkProvider = unitOfWorkProvider ?? throw new ArgumentNullException(nameof(unitOfWorkProvider));
            this.logger = logger ?? throw new ArgumentException(nameof(ILogger));
        }

        public async virtual Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
        {
            var response = default(TResponse);
            var typeName = request.GetGenericTypeName();

            try
            {
                if (this.unitOfWorkProvider.GetCurrent() != null)
                {
                    return await next();
                }

                using (var unitOfWork = this.unitOfWorkProvider.Create())
                {
                    var transactionId = Guid.NewGuid().ToString();

                    try
                    {
                        using (this.logger.BeginScope(new List<Tuple<string, object>> { new Tuple<string, object>("TransactionContext", transactionId) }))
                        {
                            this.logger.LogInformation("Begin transaction {TransactionId} for {CommandName} ({@Command})", transactionId, typeName, request);

                            response = await next();

                            this.logger.LogInformation("Commit transaction {TransactionId} for {CommandName}", transactionId, typeName);

                            await unitOfWork.CommitAsync(cancellationToken);
                        }
                    }
                    catch
                    {
                        await unitOfWork.RollbackAsync(cancellationToken);
                        throw;
                    }
                }

                return response;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error Handling transaction for {CommandName} ({@Command})", typeName, request);

                throw;
            }
        }
    }
}
