// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Query.Pipeline
{
    public abstract class ShapedQueryCompilingExpressionVisitor : ExpressionVisitor
    {
        private static readonly MethodInfo _singleMethodInfo
            = typeof(Enumerable).GetTypeInfo().GetDeclaredMethods(nameof(Enumerable.Single))
                .Single(mi => mi.GetParameters().Length == 1);

        private static readonly MethodInfo _singleOrDefaultMethodInfo
            = typeof(Enumerable).GetTypeInfo().GetDeclaredMethods(nameof(Enumerable.SingleOrDefault))
                .Single(mi => mi.GetParameters().Length == 1);

        private readonly IEntityMaterializerSource _entityMaterializerSource;
        private readonly bool _trackQueryResults;

        public ShapedQueryCompilingExpressionVisitor(IEntityMaterializerSource entityMaterializerSource, bool trackQueryResults)
        {
            _entityMaterializerSource = entityMaterializerSource;
            _trackQueryResults = trackQueryResults;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            switch (extensionExpression)
            {
                case ShapedQueryExpression shapedQueryExpression:
                    var serverEnumerable = VisitShapedQueryExpression(shapedQueryExpression);
                    switch (shapedQueryExpression.ResultType)
                    {
                        case ResultType.Enumerable:
                            return serverEnumerable;

                        case ResultType.Single:
                            return Expression.Call(
                                _singleMethodInfo.MakeGenericMethod(serverEnumerable.Type.TryGetSequenceType()),
                                serverEnumerable);

                        case ResultType.SingleWithDefault:
                            return Expression.Call(
                                _singleOrDefaultMethodInfo.MakeGenericMethod(serverEnumerable.Type.TryGetSequenceType()),
                                serverEnumerable);
                    }

                    break;
            }

            return base.VisitExtension(extensionExpression);
        }

        protected abstract Expression VisitShapedQueryExpression(ShapedQueryExpression shapedQueryExpression);

        protected virtual LambdaExpression InjectEntityMaterializer(
            LambdaExpression lambdaExpression)
        {
            return new EntityMaterializerInjectingExpressionVisitor(
                _entityMaterializerSource, _trackQueryResults).Inject(lambdaExpression);
        }

        private class EntityMaterializerInjectingExpressionVisitor : ExpressionVisitor
        {
            private static readonly ConstructorInfo _materializationContextConstructor
                = typeof(MaterializationContext).GetConstructors().Single(ci => ci.GetParameters().Length == 2);

            private static readonly PropertyInfo _dbContextMemberInfo
                = typeof(QueryContext).GetProperty(nameof(QueryContext.Context));
            private static readonly PropertyInfo _stateManagerMemberInfo
                = typeof(QueryContext).GetProperty(nameof(QueryContext.StateManager));
            private static readonly PropertyInfo _entityMemberInfo
                = typeof(InternalEntityEntry).GetProperty(nameof(InternalEntityEntry.Entity));

            private static readonly MethodInfo _tryGetEntryMethodInfo
                = typeof(IStateManager).GetMethod(nameof(IStateManager.TryGetEntry), new[] { typeof(IKey), typeof(object[]) });
            private static readonly MethodInfo _startTrackingMethodInfo
                = typeof(QueryContext).GetMethod(nameof(QueryContext.StartTracking), new[] { typeof(IEntityType), typeof(object) });

            private readonly IEntityMaterializerSource _entityMaterializerSource;
            private readonly bool _trackQueryResults;

            private readonly List<ParameterExpression> _variables = new List<ParameterExpression>();
            private readonly List<Expression> _expressions = new List<Expression>();
            private int _currentEntityIndex;

            public EntityMaterializerInjectingExpressionVisitor(
                IEntityMaterializerSource entityMaterializerSource, bool trackQueryResults)
            {
                _entityMaterializerSource = entityMaterializerSource;
                _trackQueryResults = trackQueryResults;
            }

            public LambdaExpression Inject(LambdaExpression lambdaExpression)
            {
                var modifiedBody = Visit(lambdaExpression.Body);

                if (lambdaExpression.Body == modifiedBody)
                {
                    return lambdaExpression;
                }

                _expressions.Add(modifiedBody);

                return Expression.Lambda(Expression.Block(_variables, _expressions), lambdaExpression.Parameters);
            }

            protected override Expression VisitExtension(Expression extensionExpression)
            {
                if (extensionExpression is EntityShaperExpression entityShaperExpression)
                {
                    _currentEntityIndex++;
                    var entityType = entityShaperExpression.EntityType;
                    var valueBuffer = entityShaperExpression.ValueBufferExpression;
                    var primaryKey = entityType.FindPrimaryKey();

                    if (_trackQueryResults && primaryKey == null)
                    {
                        throw new InvalidOperationException();
                    }

                    var result = Expression.Parameter(entityType.ClrType, "result" + _currentEntityIndex);
                    _variables.Add(result);

                    if (_trackQueryResults)
                    {
                        var entryVarible = Expression.Parameter(typeof(InternalEntityEntry), "entry" + _currentEntityIndex);
                        _variables.Add(entryVarible);
                        _expressions.Add(
                            Expression.Assign(
                                entryVarible,
                                Expression.Call(
                                    Expression.MakeMemberAccess(
                                        QueryCompilationContext2.QueryContextParameter,
                                        _stateManagerMemberInfo),
                                    _tryGetEntryMethodInfo,
                                    Expression.Constant(primaryKey),
                                    Expression.NewArrayInit(
                                    typeof(object),
                                    entityShaperExpression.EntityType.FindPrimaryKey().Properties
                                        .Select(p => _entityMaterializerSource.CreateReadValueExpression(
                                            entityShaperExpression.ValueBufferExpression,
                                            typeof(object),
                                            p.GetIndex(),
                                            p))))));

                        _expressions.Add(
                            Expression.Assign(
                                result,
                                Expression.Condition(
                                    Expression.NotEqual(
                                        entryVarible,
                                        Expression.Constant(default(InternalEntityEntry), typeof(InternalEntityEntry))),
                                    Expression.Convert(
                                        Expression.MakeMemberAccess(entryVarible, _entityMemberInfo),
                                        entityType.ClrType),
                                    MaterializeEntity(entityType, valueBuffer))));
                    }
                    else
                    {
                        _expressions.Add(
                            Expression.Assign(
                                result,
                                MaterializeEntity(entityType, valueBuffer)));
                    }

                    return result;
                }

                if (extensionExpression is ProjectionBindingExpression)
                {
                    return extensionExpression;
                }

                return base.VisitExtension(extensionExpression);
            }

            private Expression MaterializeEntity(IEntityType entityType, Expression valueBuffer)
            {
                var expressions = new List<Expression>();

                var materializationContext = Expression.Variable(typeof(MaterializationContext), "materializationContext" + _currentEntityIndex);

                expressions.Add(
                    Expression.Assign(
                        materializationContext,
                        Expression.New(
                            _materializationContextConstructor,
                            valueBuffer,
                            Expression.MakeMemberAccess(
                                QueryCompilationContext2.QueryContextParameter,
                                _dbContextMemberInfo))));

                var materializationExpression
                    = (BlockExpression)_entityMaterializerSource.CreateMaterializeExpression(
                        entityType,
                        "instance" + _currentEntityIndex,
                        materializationContext);

                expressions.AddRange(materializationExpression.Expressions.Take(materializationExpression.Expressions.Count - 1));

                if (_trackQueryResults)
                {
                    expressions.Add(
                        Expression.Call(
                            QueryCompilationContext2.QueryContextParameter,
                            _startTrackingMethodInfo,
                            Expression.Constant(entityType),
                            materializationExpression.Expressions.Last()));
                }

                expressions.Add(materializationExpression.Expressions.Last());

                return Expression.Block(
                    entityType.ClrType,
                    new[] { materializationContext }.Concat(materializationExpression.Variables),
                    expressions);
            }
        }
    }

}
