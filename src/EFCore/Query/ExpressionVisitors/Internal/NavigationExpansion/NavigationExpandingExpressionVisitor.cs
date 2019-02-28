// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Extensions.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class NavigationExpandingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        private IModel _model;

        public NavigationExpandingExpressionVisitor(IModel model)
        {
            _model = model;
        }

        //[DebuggerStepThrough]
        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newBody = Visit(lambdaExpression.Body);

            return newBody != lambdaExpression.Body
                ? Expression.Lambda(lambdaExpression.Type, newBody, lambdaExpression.Parameters)
                : lambdaExpression;
        }

        //[DebuggerStepThrough]
        public override Expression Visit(Expression expression)
        {
            var newExpression = base.Visit(expression);

            // TODO: convert any IEnumerable that is not NavigationExpansionExpression after visit?
            if (newExpression == expression
                && expression != null
                && expression.Type != typeof(string)
                // TODO: what about legit byte arrays?
                && expression.Type != typeof(byte[]))
            {
                var enumerableInterface = expression.Type.IsGenericType && expression.Type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    ? expression.Type
                    : expression.Type.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)).SingleOrDefault();

                if (enumerableInterface != null)
                {
                    var elementType = enumerableInterface.GetGenericArguments()[0];
                    var pendingSelectorParameter = Expression.Parameter(elementType);

                    var mapping = new List<string>();
                    var pendingSelector = Expression.Lambda(
                        new CustomRootExpression(pendingSelectorParameter, mapping, elementType),
                        pendingSelectorParameter);

                    var result = new NavigationExpansionExpression(
                        expression,
                        new NavigationExpansionExpressionState
                        {
                            CustomRootMappings = new List<List<string>> { mapping },
                            SourceMappings = new List<SourceMapping2>(),
                            CurrentParameter = pendingSelectorParameter,
                            PendingSelector = pendingSelector,
                            ApplyPendingSelector = false,
                        },
                        expression.Type);

                    return result;
                }
            }

            return newExpression;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
            {
                return navigationBindingExpression;
            }

            if (extensionExpression is CustomRootExpression customRootExpression)
            {
                return customRootExpression;
            }

            return base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            switch (methodCallExpression.Method.Name)
            {
                case nameof(Queryable.Where):
                    return ProcessWhere(methodCallExpression);

                case nameof(Queryable.Select):
                    return ProcessSelect(methodCallExpression);

                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    return ProcessOrderBy(methodCallExpression);

                case nameof(Queryable.Distinct):
                case nameof(Queryable.Skip):
                case nameof(Queryable.Take):
                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                case nameof(Queryable.Any):
                //case nameof(Queryable.Contains):
                case nameof(Queryable.OfType):
                case "AsTracking":
                case "AsNoTracking":

                    //case nameof(Queryable.DefaultIfEmpty):
                    return ProcessTerminatingOperation(methodCallExpression);

                case nameof(Queryable.Join):
                    return ProcessJoin(methodCallExpression);

                case "MaterializeCollectionNavigation":
                    var newArgument = (NavigationExpansionExpression)Visit(methodCallExpression.Arguments[1]);
                    return new NavigationExpansionExpression(newArgument, newArgument.State, methodCallExpression.Type);

                //case "MaterializeCollectionNavigation":
                //    var newArgument = (NavigationExpansionExpression)Visit(methodCallExpression.Arguments[1]);
                //    var newOperand = methodCallExpression.Update(methodCallExpression.Object, new[] { methodCallExpression.Arguments[0], newArgument });
                //    var newNavigationExpansionExpression = new NavigationExpansionExpression(newOperand, newArgument.State, newOperand.Type);
                //    return newNavigationExpansionExpression;

                default:
                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
                    {
                        var result = ProcessSelectManyWithResultOperator(methodCallExpression);

                        return result;
                    }

                    return base.VisitMethodCall(methodCallExpression);
            }
        }

        private NavigationExpansionExpressionState AdjustState(
            NavigationExpansionExpressionState state,
            NavigationExpansionExpression navigationExpansionExpression)
        {
            var currentParameter = state.CurrentParameter;
            state = navigationExpansionExpression.State;

            if (state.CurrentParameter.Name == null)
            {
                var newParameter = Expression.Parameter(state.CurrentParameter.Type, currentParameter.Name);
                state.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(state.CurrentParameter, newParameter).Visit(state.PendingSelector);
                state.CurrentParameter = newParameter;
            }

            return state;
        }

        private Expression ProcessWhere(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var predicate = methodCallExpression.Arguments[1].UnwrapQuote();
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = predicate.Parameters[0]
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            var result = FindAndApplyNavigations(source, predicate, state);
            var newPredicateBody = new NavigationPropertyUnbindingBindingExpressionVisitor2(result.state.CurrentParameter).Visit(result.lambdaBody);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.state.CurrentParameter.Type);
            var rewritten = Expression.Call(
                newMethodInfo,
                result.source,
                Expression.Lambda(
                    newPredicateBody,
                    result.state.CurrentParameter));

            return new NavigationExpansionExpression(
                rewritten,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessSelect(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var selector = methodCallExpression.Arguments[1].UnwrapQuote();
            var selectorParameter = selector.Parameters[0];
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = selectorParameter
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            var result = FindAndApplyNavigations(source, selector, state);
            result.state.PendingSelector = Expression.Lambda(result.lambdaBody, result.state.CurrentParameter);

            // TODO: unless it's identity projection
            result.state.ApplyPendingSelector = true;

            return new NavigationExpansionExpression(
                result.source,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessOrderBy(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var keySelector = methodCallExpression.Arguments[1].UnwrapQuote();
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = keySelector.Parameters[0]
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            var result = FindAndApplyNavigations(source, keySelector, state);
            var newKeySelectorBody = new NavigationPropertyUnbindingBindingExpressionVisitor2(result.state.CurrentParameter).Visit(result.lambdaBody);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.state.CurrentParameter.Type, result.lambdaBody.Type);
            var rewritten = Expression.Call(
                newMethodInfo,
                result.source,
                Expression.Lambda(
                    newKeySelectorBody,
                    result.state.CurrentParameter));

            return new NavigationExpansionExpression(
                rewritten,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessSelectManyWithResultOperator(MethodCallExpression methodCallExpression)
        {
            var outerSource = Visit(methodCallExpression.Arguments[0]);
            var outerState = new NavigationExpansionExpressionState
            {
                CurrentParameter = methodCallExpression.Arguments[2].UnwrapQuote().Parameters[0]
            };

            if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            {
                outerSource = outerNavigationExpansionExpression.Operand;
                outerState = AdjustState(outerState, outerNavigationExpansionExpression);
            }

            var collectionSelector = methodCallExpression.Arguments[1].UnwrapQuote();
            var collectionSelectorResult = FindAndApplyNavigations(outerSource, collectionSelector, outerState);
            if (collectionSelectorResult.lambdaBody is NavigationExpansionExpression collectionSelectorNavigationExpansionExpression)
            {
                //var correlationChecker = new CorrelationChecker(collectionSelector.Parameters[0]);
                //correlationChecker.Visit(collectionSelector.Body);
                //if (!correlationChecker.Correlated)
                {
                    // collection is uncorrelated with the source -> expand into subquery and keep as SelectMany
                    var collectionSelectorState = collectionSelectorNavigationExpansionExpression.State;
                    if (collectionSelectorState.CurrentParameter.Name == null
                        || collectionSelectorState.CurrentParameter.Name != methodCallExpression.Arguments[1].UnwrapQuote().Parameters[0].Name)
                    {
                        // TODO: should we rename the second parameter according to the second parameter of the result selector instead?
                        var newParameter = Expression.Parameter(collectionSelectorState.CurrentParameter.Type, methodCallExpression.Arguments[2].UnwrapQuote().Parameters[1].Name);
                        collectionSelectorState.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(collectionSelectorState.CurrentParameter, newParameter).Visit(collectionSelectorState.PendingSelector);
                        collectionSelectorState.CurrentParameter = newParameter;
                    }

                    var resultSelector = methodCallExpression.Arguments[2].UnwrapQuote();
                    var resultSelectorRemap = RemapTwoArgumentResultSelector(resultSelector, outerState, collectionSelectorNavigationExpansionExpression.State);

                    var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                        outerState.CurrentParameter.Type,
                        collectionSelectorState.CurrentParameter.Type,
                        resultSelectorRemap.lambda.Body.Type);

                    // in case collection selector body is IQueryable, we need to adjust the type to IEnumerable, to match the SelectMany signature
                    // therefore the delegate type is specified explicitly
                    var newCollectionSelectorLambda = Expression.Lambda(
                        newMethodInfo.GetParameters()[1].ParameterType.GetGenericArguments()[0],
                        collectionSelectorNavigationExpansionExpression.Operand,
                        outerState.CurrentParameter);

                    var rewritten = Expression.Call(
                        newMethodInfo,
                        outerSource,
                        newCollectionSelectorLambda,
                        resultSelectorRemap.lambda);

                    // temporarily change selector to ti => ti for purpose of finding & expanding navigations in the pending selector lambda itself
                    var pendingSelector = resultSelectorRemap.state.PendingSelector;
                    resultSelectorRemap.state.PendingSelector = Expression.Lambda(resultSelectorRemap.state.PendingSelector.Parameters[0], resultSelectorRemap.state.PendingSelector.Parameters[0]);
                    var result = FindAndApplyNavigations(rewritten, pendingSelector, resultSelectorRemap.state);
                    result.state.PendingSelector = Expression.Lambda(result.lambdaBody, result.state.CurrentParameter);

                    return new NavigationExpansionExpression(
                        result.source,
                        result.state,
                        methodCallExpression.Type);
                }

                //var correlationPredicateExtractor = new CorrelationPredicateExtractingExpressionVisitor(collectionSelectorResult.state.CurrentParameter);
                //var collectionSelectorWithoutCorelationPredicate = (NavigationExpansionExpression)correlationPredicateExtractor.Visit(collectionSelectorNavigationExpansionExpression);
                //if (correlationPredicateExtractor.CorrelationPredicate != null)
                //{
                //    // if collection selector is correlated then outer state then outerState should be updated with new values (as the collectionSelector is essencially extension of outer)
                //    outerState = collectionSelectorResult.state;
                //    outerSource = collectionSelectorResult.source;

                //    var outerKeyLambdaBody = new NavigationPropertyUnbindingBindingExpressionVisitor2(outerState.CurrentParameter).Visit(correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Left);
                //    var outerKeyLambda = Expression.Lambda(outerKeyLambdaBody, outerState.CurrentParameter);

                //    // inner key selector needs to be remapped - when correlation predicate was being built it was based on "naked" entity from the collection
                //    // however there could have been navigations afterwards which would have caused types to be changed to TransparentIdentifiers
                //    // all necessary mappings should be stored in the inner NavigationExpansionExpression state.
                //    var innerKeyLambda = Expression.Lambda(correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Right, correlationPredicateExtractor.CorrelatedCollectionParameter);
                //    innerKeyLambda = ExpressionExtensions.CombineAndRemapLambdas(collectionSelectorWithoutCorelationPredicate.State.PendingSelector2, innerKeyLambda);
                //    innerKeyLambda = (LambdaExpression)new NavigationPropertyUnbindingBindingExpressionVisitor2(collectionSelectorWithoutCorelationPredicate.State.CurrentParameter).Visit(innerKeyLambda);

                //    var remapResult = RemapTwoArgumentResultSelector3(methodCallExpression.Arguments[2].UnwrapQuote(), outerState, collectionSelectorWithoutCorelationPredicate.State);

                //    var joinMethodInfo = QueryableJoinMethodInfo.MakeGenericMethod(
                //        outerState.CurrentParameter.Type,
                //        collectionSelectorWithoutCorelationPredicate.State.CurrentParameter.Type,
                //        innerKeyLambda.Body.Type,
                //        remapResult.lambda.Body.Type);

                //    var rewritten = Expression.Call(
                //        joinMethodInfo,
                //        outerSource,
                //        collectionSelectorWithoutCorelationPredicate.Operand,
                //        outerKeyLambda,
                //        innerKeyLambda,
                //        remapResult.lambda);

                //    // TODO: should we run FindAndApplyNavigations on the result selector as well? - to expand all the navigations there, just like we do for uncorrelated case?

                //    return new NavigationExpansionExpression(
                //        rewritten,
                //        remapResult.state,
                //        methodCallExpression.Type);
                //}
                //else
                //{
                //    throw new InvalidOperationException("correlation predicate not found. Expression: " + methodCallExpression);
                //}
            }

            throw new InvalidOperationException("collection selector was not NavigationExpansionExpression");
        }

        //private class CorrelationChecker : NavigationExpansionExpressionVisitorBase
        //{
        //    private ParameterExpression _rootParameter;

        //    public bool Correlated { get; private set; } = false;

        //    public CorrelationChecker(ParameterExpression rootParameter)
        //    {
        //        _rootParameter = rootParameter;
        //    }

        //    protected override Expression VisitParameter(ParameterExpression parameterExpression)
        //    {
        //        if (parameterExpression == _rootParameter)
        //        {
        //            Correlated = true;
        //        }

        //        return parameterExpression;
        //    }
        //}

        private Expression ProcessJoin(MethodCallExpression methodCallExpression)
        {
            var outerSource = Visit(methodCallExpression.Arguments[0]);
            var innerSource = Visit(methodCallExpression.Arguments[1]);

            var outerKeySelector = methodCallExpression.Arguments[2].UnwrapQuote();
            var innerKeySelector = methodCallExpression.Arguments[3].UnwrapQuote();
            var resultSelector = methodCallExpression.Arguments[4].UnwrapQuote();

            var outerState = new NavigationExpansionExpressionState
            {
                CurrentParameter = outerKeySelector.Parameters[0]
            };

            var innerState = new NavigationExpansionExpressionState
            {
                CurrentParameter = innerKeySelector.Parameters[0]
            };

            if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            {
                outerSource = outerNavigationExpansionExpression.Operand;
                outerState = AdjustState(outerState, outerNavigationExpansionExpression);
            }

            if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            {
                innerSource = innerNavigationExpansionExpression.Operand;
                innerState = AdjustState(innerState, innerNavigationExpansionExpression);
            }

            var outerResult = FindAndApplyNavigations(outerSource, outerKeySelector, outerState);
            var innerResult = FindAndApplyNavigations(innerSource, innerKeySelector, innerState);

            var newOuterKeySelectorBody = new NavigationPropertyUnbindingBindingExpressionVisitor2(outerResult.state.CurrentParameter).Visit(outerResult.lambdaBody);
            var newInnerKeySelectorBody = new NavigationPropertyUnbindingBindingExpressionVisitor2(innerResult.state.CurrentParameter).Visit(innerResult.lambdaBody);

            var resultSelectorRemap = RemapTwoArgumentResultSelector(resultSelector, outerResult.state, innerResult.state);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                outerResult.state.CurrentParameter.Type,
                innerResult.state.CurrentParameter.Type,
                outerResult.lambdaBody.Type,
                resultSelectorRemap.lambda.Body.Type);

            var rewritten = Expression.Call(
                newMethodInfo,
                outerResult.source,
                innerResult.source,
                Expression.Lambda(newOuterKeySelectorBody, outerResult.state.CurrentParameter),
                Expression.Lambda(newInnerKeySelectorBody, innerResult.state.CurrentParameter),
                Expression.Lambda(resultSelectorRemap.lambda.Body, outerResult.state.CurrentParameter, innerResult.state.CurrentParameter));

            // temporarily change selector to ti => ti for purpose of finding & expanding navigations in the pending selector lambda itself
            var pendingSelector = resultSelectorRemap.state.PendingSelector;
            resultSelectorRemap.state.PendingSelector = Expression.Lambda(resultSelectorRemap.state.PendingSelector.Parameters[0], resultSelectorRemap.state.PendingSelector.Parameters[0]);
            var result = FindAndApplyNavigations(rewritten, pendingSelector, resultSelectorRemap.state);
            result.state.PendingSelector = Expression.Lambda(result.lambdaBody, result.state.CurrentParameter);

            return new NavigationExpansionExpression(
                result.source,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessGroupJoin(MethodCallExpression methodCallExpression)
        {
            return methodCallExpression;
        }

        private Expression ProcessTerminatingOperation(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = Expression.Parameter(
                    source.Type.GetElementType() ?? source.Type.GetGenericArguments()[0]/*,
                    (source.Type.GetElementType() ?? source.Type.GetGenericArguments()[0]).GenerateParameterName()*/)
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;

                if (state.ApplyPendingSelector)
                {
                    var unbinder = new NavigationPropertyUnbindingBindingExpressionVisitor2(state.CurrentParameter);
                    var newSelectorBody = unbinder.Visit(state.PendingSelector.Body);

                    // TODO: test cases with casts, e.g. orders.Select(o => new { foo = (VipCustomer)o.Customer }).Distinct()
                    var entityTypeOverride = methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)
                        ? _model.FindEntityType(methodCallExpression.Method.GetGenericArguments()[0])
                        : null;

                    var pssmg = new PendingSelectorSourceMappingGenerator(state.PendingSelector.Parameters[0], entityTypeOverride);
                    pssmg.Visit(state.PendingSelector);

                    var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                        state.CurrentParameter.Type,
                        newSelectorBody.Type);

                    var result = Expression.Call(
                        selectorMethodInfo,
                        navigationExpansionExpression.Operand,
                        Expression.Lambda(newSelectorBody, state.CurrentParameter));

                    var newPendingSelectorParameter = Expression.Parameter(newSelectorBody.Type);
                    var customRootMapping = new List<string>();

                    // if the top level was navigation binding, then we are guaranteed to have exactly one source mapping in for the new pending selector
                    var newPendingSelectorBody = state.PendingSelector.Body is NavigationBindingExpression2 binding
                        ? (Expression)new NavigationBindingExpression2(
                            newPendingSelectorParameter,
                            pssmg.BindingToSourceMapping[binding].NavigationTree,
                            pssmg.BindingToSourceMapping[binding].RootEntityType,
                            pssmg.BindingToSourceMapping[binding],
                            newPendingSelectorParameter.Type)
                        : new CustomRootExpression(newPendingSelectorParameter, customRootMapping, newPendingSelectorParameter.Type);

                    // TODO: only apply custom root mapping for parameters that are not root!
                    state = new NavigationExpansionExpressionState
                    {
                        ApplyPendingSelector = false,
                        CustomRootMappings = new List<List<string>> { customRootMapping },
                        CurrentParameter = newPendingSelectorParameter,
                        PendingSelector = Expression.Lambda(newPendingSelectorBody, newPendingSelectorParameter),
                        SourceMappings = pssmg.SourceMappings
                    };

                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)

                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)

                        || methodCallExpression.Method.Name == "AsTracking"
                        || methodCallExpression.Method.Name == "AsNoTracking")
                    {
                        var newMethod = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                            result.Type.GetGenericArguments()[0]);

                        source = Expression.Call(newMethod, new[] { result });
                    }
                    else if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSkipMethodInfo)
                        /*|| methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains)*/)
                    {
                        // TODO: is it necessary to visit the argument, or can we just pass it as is?
                        var newArgument = Visit(methodCallExpression.Arguments[1]);

                        source = methodCallExpression.Update(methodCallExpression.Object, new[] { result, newArgument });
                    }
                    else
                    {
                        throw new InvalidOperationException("Unsupported method " + methodCallExpression.Method.Name);
                    }
                }
                else
                {
                    // TODO: need to run thru Expression.Update?
                    source = methodCallExpression;
                    var newParameter = Expression.Parameter(state.CurrentParameter.Type);
                    state.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(state.CurrentParameter, newParameter).Visit(state.PendingSelector);
                    state.CurrentParameter = newParameter;
                }

                // TODO: should we be reusing state?
                return new NavigationExpansionExpression(
                    source,
                    state,
                    methodCallExpression.Type);
            }

            // we should never hit this
            return methodCallExpression;
        }

        protected override Expression VisitConstant(ConstantExpression constantExpression)
        {
            if (constantExpression.Value != null
                && constantExpression.Value.GetType().IsGenericType
                && constantExpression.Value.GetType().GetGenericTypeDefinition() == typeof(EntityQueryable<>))
            {
                var elementType = constantExpression.Value.GetType().GetGenericArguments()[0];
                var entityType = _model.FindEntityType(elementType);

                var sourceMapping = new SourceMapping2
                {
                    RootEntityType = entityType,
                };

                var navigationTreeRoot = NavigationTreeNode2.CreateRoot(sourceMapping, fromMapping: new List<string>(), optional: false);
                sourceMapping.NavigationTree = navigationTreeRoot;

                var pendingSelectorParameter = Expression.Parameter(entityType.ClrType);
                var pendingSelector = Expression.Lambda(
                    new NavigationBindingExpression2(
                        pendingSelectorParameter,
                        navigationTreeRoot,
                        entityType,
                        sourceMapping,
                        pendingSelectorParameter.Type),
                    pendingSelectorParameter);

                var result = new NavigationExpansionExpression(
                    constantExpression,
                    new NavigationExpansionExpressionState
                    {
                        CustomRootMappings = new List<List<string>>(),
                        SourceMappings = new List<SourceMapping2> { sourceMapping },
                        CurrentParameter = pendingSelectorParameter,
                        PendingSelector = pendingSelector,
                        ApplyPendingSelector = false,
                    },
                    constantExpression.Type);

                return result;
            }

            //if (constantExpression.Value != null
            //    && constantExpression.Type.IsArray)
            //{
            //    var elementType = constantExpression.Type.GetElementType();
            //    var pendingSelectorParameter = Expression.Parameter(elementType);

            //    var mapping = new List<string>();
            //    var pendingSelector = Expression.Lambda(
            //        new CustomRootExpression(pendingSelectorParameter, mapping, elementType),
            //        pendingSelectorParameter);

            //    var result = new NavigationExpansionExpression(
            //        constantExpression,
            //        new NavigationExpansionExpressionState
            //        {
            //            CustomRootMappings = new List<List<string>> { mapping },
            //            SourceMappings = new List<SourceMapping>(),
            //            SourceMappings2 = new List<SourceMapping2>(),
            //            CurrentParameter = pendingSelectorParameter,
            //            PendingSelector2 = pendingSelector,
            //            ApplyPendingSelector = false,
            //        },
            //        constantExpression.Type);

            //    return result;
            //}

            return base.VisitConstant(constantExpression);
        }

        private (Expression source, Expression lambdaBody, NavigationExpansionExpressionState state) FindAndApplyNavigations(
            Expression source,
            LambdaExpression lambda,
            NavigationExpansionExpressionState state)
        {
            var remappedLambdaBody = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, lambda).Body;

            var binder = new NavigationPropertyBindingExpressionVisitor2(
                state.PendingSelector.Parameters[0],
                state.SourceMappings);

            var boundLambdaBody = binder.Visit(remappedLambdaBody);
            var boundLambda = Expression.Lambda(boundLambdaBody, state.CurrentParameter);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(state.CurrentParameter);
            boundLambdaBody = cnrev.Visit(boundLambdaBody);
            boundLambdaBody = Visit(boundLambdaBody);

            var result = (source, parameter: state.CurrentParameter);
            var applyPendingSelector = state.ApplyPendingSelector;

            foreach (var sourceMapping in state.SourceMappings)
            {
                if (sourceMapping.NavigationTree.Flatten().Any(n => !n.Expanded))
                {
                    foreach (var navigationTree in sourceMapping.NavigationTree.Children)
                    {
                        result = AddNavigationJoin2(
                            result.source,
                            result.parameter,
                            sourceMapping,
                            navigationTree,
                            state,
                            new List<INavigation>());
                    }

                    applyPendingSelector = true;
                }
            }

            var pendingSelector = state.PendingSelector;
            if (state.CurrentParameter != result.parameter)
            {
                var pendingSelectorBody = new ExpressionReplacingVisitor(state.CurrentParameter, result.parameter).Visit(state.PendingSelector.Body);
                pendingSelector = Expression.Lambda(pendingSelectorBody, result.parameter);
                boundLambdaBody = new ExpressionReplacingVisitor(state.CurrentParameter, result.parameter).Visit(boundLambdaBody);
            }

            var newState = new NavigationExpansionExpressionState
            {
                CurrentParameter = result.parameter,
                CustomRootMappings = state.CustomRootMappings,
                SourceMappings = state.SourceMappings,
                PendingSelector = pendingSelector,
                ApplyPendingSelector = applyPendingSelector,
            };

            // TODO: improve this (maybe a helper method?)
            if (source.Type.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)
                && result.source.Type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                var toOrderedMethod = typeof(NavigationExpansionExpression).GetMethod(nameof(NavigationExpansionExpression.ToOrdered)).MakeGenericMethod(result.source.Type.GetGenericArguments()[0]);
                var toOrderedCall = Expression.Call(toOrderedMethod, result.source);

                return (source: toOrderedCall, lambdaBody: boundLambdaBody, state: newState);
            }

            return (result.source, lambdaBody: boundLambdaBody, state: newState);
        }

        private (Expression source, ParameterExpression parameter) AddNavigationJoin2(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            SourceMapping2 sourceMapping2,
            NavigationTreeNode2 navigationTree2,
            NavigationExpansionExpressionState state,
            List<INavigation> navigationPath)
        {
            if (!navigationTree2.Expanded)
            {
                // TODO: hack - if we wrapped collection around MaterializeCollectionNavigation during collection rewrite, unwrap that call when applying navigations on top
                if (sourceExpression is MethodCallExpression sourceMethodCall
                    && sourceMethodCall.Method.Name == "MaterializeCollectionNavigation")
                {
                    sourceExpression = sourceMethodCall.Arguments[1];
                }

                var navigation = navigationTree2.Navigation;
                var sourceType = sourceExpression.Type.GetGenericArguments()[0];
                var navigationTargetEntityType = navigation.GetTargetType();

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, null, navigationTree2.Parent.ToMapping);

                var outerKeySelectorBody = CreateKeyAccessExpression(
                    transparentIdentifierAccessorExpression,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.Properties
                        : navigation.ForeignKey.PrincipalKey.Properties,
                    addNullCheck: navigationTree2.Parent != null && navigationTree2.Parent.Optional);

                var innerKeySelectorParameterType = navigationTargetEntityType.ClrType;
                var innerKeySelectorParameter = Expression.Parameter(
                    innerKeySelectorParameterType,
                    parameterExpression.Name + "." + navigationTree2.Navigation.Name);

                var innerKeySelectorBody = CreateKeyAccessExpression(
                    innerKeySelectorParameter,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.PrincipalKey.Properties
                        : navigation.ForeignKey.Properties);

                if (outerKeySelectorBody.Type.IsNullableType()
                    && !innerKeySelectorBody.Type.IsNullableType())
                {
                    innerKeySelectorBody = Expression.Convert(innerKeySelectorBody, outerKeySelectorBody.Type);
                }
                else if (innerKeySelectorBody.Type.IsNullableType()
                    && !outerKeySelectorBody.Type.IsNullableType())
                {
                    outerKeySelectorBody = Expression.Convert(outerKeySelectorBody, innerKeySelectorBody.Type);
                }

                var outerKeySelector = Expression.Lambda(
                    outerKeySelectorBody,
                    outerKeySelectorParameter);

                var innerKeySelector = Expression.Lambda(
                    innerKeySelectorBody,
                    innerKeySelectorParameter);

                var oldParameterExpression = parameterExpression;
                if (navigationTree2.Optional)
                {
                    var groupingType = typeof(IEnumerable<>).MakeGenericType(navigationTargetEntityType.ClrType);
                    var groupJoinResultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, groupingType);

                    var groupJoinMethodInfo = QueryableGroupJoinMethodInfo.MakeGenericMethod(
                        sourceType,
                        navigationTargetEntityType.ClrType,
                        outerKeySelector.Body.Type,
                        groupJoinResultType);

                    var resultSelectorOuterParameterName = outerParameter.Name;
                    var resultSelectorOuterParameter = Expression.Parameter(sourceType, resultSelectorOuterParameterName);

                    var resultSelectorInnerParameterName = innerKeySelectorParameter.Name;
                    var resultSelectorInnerParameter = Expression.Parameter(groupingType, resultSelectorInnerParameterName);

                    var groupJoinResultTransparentIdentifierCtorInfo
                        = groupJoinResultType.GetTypeInfo().GetConstructors().Single();

                    var groupJoinResultSelector = Expression.Lambda(
                        Expression.New(groupJoinResultTransparentIdentifierCtorInfo, resultSelectorOuterParameter, resultSelectorInnerParameter),
                        resultSelectorOuterParameter,
                        resultSelectorInnerParameter);

                    var groupJoinMethodCall
                        = Expression.Call(
                            groupJoinMethodInfo,
                            sourceExpression,
                            entityQueryable,
                            outerKeySelector,
                            innerKeySelector,
                            groupJoinResultSelector);

                    var selectManyResultType = typeof(TransparentIdentifier<,>).MakeGenericType(groupJoinResultType, navigationTargetEntityType.ClrType);

                    var selectManyMethodInfo = QueryableSelectManyWithResultOperatorMethodInfo.MakeGenericMethod(
                        groupJoinResultType,
                        navigationTargetEntityType.ClrType,
                        selectManyResultType);

                    var defaultIfEmptyMethodInfo = EnumerableDefaultIfEmptyMethodInfo.MakeGenericMethod(navigationTargetEntityType.ClrType);

                    var selectManyCollectionSelectorParameter = Expression.Parameter(groupJoinResultType);
                    var selectManyCollectionSelector = Expression.Lambda(
                        Expression.Call(
                            defaultIfEmptyMethodInfo,
                            Expression.Field(selectManyCollectionSelectorParameter, nameof(TransparentIdentifier<object, object>.Inner))),
                        selectManyCollectionSelectorParameter);

                    var selectManyResultTransparentIdentifierCtorInfo
                        = selectManyResultType.GetTypeInfo().GetConstructors().Single();

                    // TODO: dont reuse parameters here?
                    var selectManyResultSelector = Expression.Lambda(
                        Expression.New(selectManyResultTransparentIdentifierCtorInfo, selectManyCollectionSelectorParameter, innerKeySelectorParameter),
                        selectManyCollectionSelectorParameter,
                        innerKeySelectorParameter);

                    var selectManyMethodCall
                        = Expression.Call(selectManyMethodInfo,
                        groupJoinMethodCall,
                        selectManyCollectionSelector,
                        selectManyResultSelector);

                    sourceType = selectManyResultSelector.ReturnType;
                    sourceExpression = selectManyMethodCall;

                    var transparentIdentifierParameterName = resultSelectorInnerParameterName;
                    var transparentIdentifierParameter = Expression.Parameter(selectManyResultSelector.ReturnType, transparentIdentifierParameterName);
                    parameterExpression = transparentIdentifierParameter;
                }
                else
                {
                    var joinMethodInfo = QueryableJoinMethodInfo.MakeGenericMethod(
                        sourceType,
                        navigationTargetEntityType.ClrType,
                        outerKeySelector.Body.Type,
                        resultType);

                    var resultSelectorOuterParameterName = outerParameter.Name;
                    var resultSelectorOuterParameter = Expression.Parameter(sourceType, resultSelectorOuterParameterName);

                    var resultSelectorInnerParameterName = innerKeySelectorParameter.Name;
                    var resultSelectorInnerParameter = Expression.Parameter(navigationTargetEntityType.ClrType, resultSelectorInnerParameterName);

                    var transparentIdentifierCtorInfo
                        = resultType.GetTypeInfo().GetConstructors().Single();

                    var resultSelector = Expression.Lambda(
                        Expression.New(transparentIdentifierCtorInfo, resultSelectorOuterParameter, resultSelectorInnerParameter),
                        resultSelectorOuterParameter,
                        resultSelectorInnerParameter);

                    var joinMethodCall = Expression.Call(
                        joinMethodInfo,
                        sourceExpression,
                        entityQueryable,
                        outerKeySelector,
                        innerKeySelector,
                        resultSelector);

                    sourceType = resultSelector.ReturnType;
                    sourceExpression = joinMethodCall;

                    var transparentIdentifierParameterName = resultSelectorInnerParameterName;
                    var transparentIdentifierParameter = Expression.Parameter(resultSelector.ReturnType, transparentIdentifierParameterName);
                    parameterExpression = transparentIdentifierParameter;
                }

                // remap navigation 'To' paths -> for this navigation prepend "Inner", for every other (already expanded) navigation prepend "Outer"
                navigationTree2.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                foreach (var sourceMapping in state.SourceMappings)
                {
                    foreach (var navigationTreeNode in sourceMapping.NavigationTree.Flatten().Where(n => n.Expanded && n != navigationTree2))
                    {
                        navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        if (navigationTree2.Optional)
                        {
                            navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        }
                    }
                }

                foreach (var customRootMapping in state.CustomRootMappings)
                {
                    customRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    if (navigationTree2.Optional)
                    {
                        customRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    }
                }

                navigationTree2.Expanded = true;
                navigationPath.Add(navigation);
            }
            else
            {
                navigationPath.Add(navigationTree2.Navigation);
            }

            var result = (source: sourceExpression, parameter: parameterExpression);
            foreach (var child in navigationTree2.Children)
            {
                result = AddNavigationJoin2(
                    result.source,
                    result.parameter,
                    sourceMapping2,
                    child,
                    state,
                    navigationPath.ToList());
            }

            return result;
        }

        private void RemapNavigationChain(NavigationTreeNode2 navigationTreeNode, bool optional)
        {
            if (navigationTreeNode != null)
            {
                navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                if (optional)
                {
                    navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                }

                RemapNavigationChain(navigationTreeNode.Parent, optional);
            }
        }

        private (LambdaExpression lambda, NavigationExpansionExpressionState state) RemapTwoArgumentResultSelector(
            LambdaExpression resultSelector,
            NavigationExpansionExpressionState outerState,
            NavigationExpansionExpressionState innerState)
        {
            var resultSelectorBody = resultSelector.Body;
            var remappedResultSelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, resultSelector.Parameters[0]);
            remappedResultSelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, remappedResultSelector, remappedResultSelector.Parameters[1]);

            var outerBinder = new NavigationPropertyBindingExpressionVisitor2(
                outerState.CurrentParameter,
                outerState.SourceMappings);

            var innerBinder = new NavigationPropertyBindingExpressionVisitor2(
                innerState.CurrentParameter,
                innerState.SourceMappings);

            var boundResultSelectorBody = outerBinder.Visit(remappedResultSelector.Body);
            boundResultSelectorBody = innerBinder.Visit(boundResultSelectorBody);

            foreach (var outerCustomRootMapping in outerState.CustomRootMappings)
            {
                outerCustomRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
            }

            foreach (var outerSourceMapping in outerState.SourceMappings)
            {
                foreach (var navigationTreeNode in outerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
                {
                    navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    foreach (var fromMapping in navigationTreeNode.FromMappings)
                    {
                        fromMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    }
                }
            }

            foreach (var innerCustomRootMapping in innerState.CustomRootMappings)
            {
                innerCustomRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
            }

            foreach (var innerSourceMapping in innerState.SourceMappings)
            {
                foreach (var navigationTreeNode in innerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
                {
                    navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                    foreach (var fromMapping in navigationTreeNode.FromMappings)
                    {
                        fromMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                    }
                }
            }

            var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(outerState.CurrentParameter.Type, innerState.CurrentParameter.Type);
            var transparentIdentifierCtorInfo = resultType.GetTypeInfo().GetConstructors().Single();
            var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");

            var newPendingSelectorBody = new ExpressionReplacingVisitor(outerState.CurrentParameter, transparentIdentifierParameter).Visit(boundResultSelectorBody);
            newPendingSelectorBody = new ExpressionReplacingVisitor(innerState.CurrentParameter, transparentIdentifierParameter).Visit(newPendingSelectorBody);

            var newState = new NavigationExpansionExpressionState
            {
                ApplyPendingSelector = true,
                CurrentParameter = transparentIdentifierParameter,
                CustomRootMappings = outerState.CustomRootMappings.Concat(innerState.CustomRootMappings).ToList(),
                PendingSelector = Expression.Lambda(newPendingSelectorBody, transparentIdentifierParameter),
                SourceMappings = outerState.SourceMappings.Concat(innerState.SourceMappings).ToList()
            };

            var lambda = Expression.Lambda(
                Expression.New(transparentIdentifierCtorInfo, outerState.CurrentParameter, innerState.CurrentParameter),
                outerState.CurrentParameter,
                innerState.CurrentParameter);

            return (lambda, state: newState);
        }

        //private class NavigationBindingTwoArgumentRemapper : NavigationExpansionExpressionVisitorBase
        //{
        //    private ParameterExpression _previousRootParameter;
        //    private ParameterExpression _newRootParameter;
        //    private Expression _newOperandRoot;

        //    public NavigationBindingTwoArgumentRemapper(
        //        ParameterExpression previousRootParameter,
        //        ParameterExpression newRootParameter,
        //        Expression newOperandRoot)
        //    {
        //        _previousRootParameter = previousRootParameter;
        //        _newRootParameter = newRootParameter;
        //        _newOperandRoot = newOperandRoot;
        //    }

        //    protected override Expression VisitExtension(Expression extensionExpression)
        //    {
        //        // remaps root parameter into transparent identifier parameter and the root part of the operand into ti.Outer or ti.Inner
        //        if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression
        //            && navigationBindingExpression.RootParameter == _previousRootParameter)
        //        {
        //            return _newRootParameter != navigationBindingExpression.RootParameter
        //                ? new NavigationBindingExpression2(
        //                    _newRootParameter,
        //                    navigationBindingExpression.NavigationTreeNode,
        //                    navigationBindingExpression.EntityType,
        //                    navigationBindingExpression.SourceMapping,
        //                    navigationBindingExpression.Type)
        //                : navigationBindingExpression;
        //        }

        //        return base.VisitExtension(extensionExpression);
        //    }
        //}

        // TODO: DRY
        private static Expression CreateKeyAccessExpression(
            Expression target, IReadOnlyList<IProperty> properties, bool addNullCheck = false)
            => properties.Count == 1
                ? CreatePropertyExpression(target, properties[0], addNullCheck)
                : Expression.New(
                    AnonymousObject.AnonymousObjectCtor,
                    Expression.NewArrayInit(
                        typeof(object),
                        properties
                            .Select(p => Expression.Convert(CreatePropertyExpression(target, p, addNullCheck), typeof(object)))
                            .Cast<Expression>()
                            .ToArray()));

        // TODO: DRY
        private static Expression CreatePropertyExpression(Expression target, IProperty property, bool addNullCheck)
        {
            var propertyExpression = target.CreateEFPropertyExpression(property, makeNullable: false);

            var propertyDeclaringType = property.DeclaringType.ClrType;
            if (propertyDeclaringType != target.Type
                && target.Type.GetTypeInfo().IsAssignableFrom(propertyDeclaringType.GetTypeInfo()))
            {
                if (!propertyExpression.Type.IsNullableType())
                {
                    propertyExpression = Expression.Convert(propertyExpression, propertyExpression.Type.MakeNullable());
                }

                return Expression.Condition(
                    Expression.TypeIs(target, propertyDeclaringType),
                    propertyExpression,
                    Expression.Constant(null, propertyExpression.Type));
            }

            return addNullCheck
                ? new NullConditionalExpression(target, propertyExpression)
                : propertyExpression;
        }

        // TODO: DRY
        private Expression BuildTransparentIdentifierAccessorExpression(Expression source, List<string> initialPath, List<string> accessorPath)
        {
            var result = source;

            var fullPath = initialPath != null
                ? initialPath.Concat(accessorPath).ToList()
                : accessorPath;

            if (fullPath != null)
            {
                foreach (var accessorPathElement in fullPath)
                {
                    // TODO: nasty hack, clean this up!!!!
                    if (result.Type.GetProperties().Any(p => p.Name == accessorPathElement))
                    {
                        result = Expression.Property(result, accessorPathElement);
                    }
                    else
                    {
                        result = Expression.Field(result, accessorPathElement);
                    }
                }
            }

            return result;
        }

        //private class PendingSelectorUpdatingExpressionVisitor : ExpressionVisitor
        //{
        //    private ParameterExpression _oldParameter;
        //    private ParameterExpression _newParameter;
        //    private bool _optional;

        //    public PendingSelectorUpdatingExpressionVisitor(ParameterExpression oldParameter, ParameterExpression newParameter, bool optional)
        //    {
        //        _oldParameter = oldParameter;
        //        _newParameter = newParameter;
        //        _optional = optional;
        //    }

        //    protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        //    {
        //        // TODO: combine this with navigation replacing expression visitor? logic is the same
        //        var newParameters = new List<ParameterExpression>();
        //        var parameterChanged = false;

        //        foreach (var parameter in lambdaExpression.Parameters)
        //        {
        //            if (parameter == _oldParameter)
        //            {
        //                newParameters.Add(_newParameter);
        //                parameterChanged = true;
        //            }
        //            else
        //            {
        //                newParameters.Add(parameter);
        //            }
        //        }

        //        var newBody = Visit(lambdaExpression.Body);

        //        return parameterChanged || newBody != lambdaExpression.Body
        //            ? Expression.Lambda(newBody, newParameters)
        //            : lambdaExpression;
        //    }

        //    protected override Expression VisitParameter(ParameterExpression parameterExpression)
        //        => parameterExpression == _oldParameter
        //        ? _optional
        //            ? Expression.Field(
        //                Expression.Field(
        //                    _newParameter,
        //                    "Outer"),
        //                "Outer")
        //            : Expression.Field(
        //                _newParameter,
        //                "Outer")
        //        : (Expression)parameterExpression;
        //}

        //private class FoundNavigationSelectorRemapper : ExpressionVisitor
        //{
        //    private ParameterExpression _rootParameter;
        //    private List<string> _currentPath = new List<string>();

        //    private Dictionary<NavigationTreeNode2, List<List<string>>> _newNavigationMappings;

        //    public FoundNavigationSelectorRemapper(ParameterExpression rootParameter)
        //    {
        //        _rootParameter = rootParameter;
        //    }

        //    public void Remap(Expression expression)
        //    {
        //        _newNavigationMappings = new Dictionary<NavigationTreeNode2, List<List<string>>>();

        //        Visit(expression);

        //        // TODO: clear old mappings, some navigations may/should become inaccessible
        //        foreach (var mappingElement in _newNavigationMappings)
        //        {
        //            mappingElement.Key.FromMappings = mappingElement.Value;
        //        }
        //    }

        //    // prune these nodes, we only want to look for entities accessible in the result
        //    protected override Expression VisitMember(MemberExpression memberExpression)
        //        => memberExpression;

        //    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        //        => methodCallExpression;

        //    protected override Expression VisitBinary(BinaryExpression binaryExpression)
        //        => binaryExpression;

        //    protected override Expression VisitNew(NewExpression newExpression)
        //    {
        //        // TODO: when constructing a DTO, there will be arguments present, but no members - is it correct to just skip in this case?
        //        if (newExpression.Members != null)
        //        {
        //            for (var i = 0; i < newExpression.Arguments.Count; i++)
        //            {
        //                _currentPath.Add(newExpression.Members[i].Name);
        //                Visit(newExpression.Arguments[i]);
        //                _currentPath.RemoveAt(_currentPath.Count - 1);
        //            }
        //        }

        //        return newExpression;
        //    }

        //    protected override Expression VisitExtension(Expression extensionExpression)
        //    {
        //        if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
        //        {
        //            if (navigationBindingExpression.RootParameter == _rootParameter)
        //            {
        //                var navigationTreeNode = navigationBindingExpression.NavigationTreeNode;
        //                //if (navigationTreeNode == null)
        //                //{
        //                //    // TODO: DRY this - make source mapping a NavigationTreeNode without Navigation property
        //                //    if (!_newRootMappings.ContainsKey(navigationBindingExpression.SourceMapping))
        //                //    {
        //                //        _newRootMappings[navigationBindingExpression.SourceMapping] = new List<List<string>>();
        //                //    }

        //                //    _newRootMappings[navigationBindingExpression.SourceMapping].Add(_currentPath.ToList());

        //                //    foreach (var rootFromMapping in navigationBindingExpression.SourceMapping.RootFromMappings)
        //                //    {
        //                //        foreach (var foundNavigation in navigationBindingExpression.SourceMapping.FoundNavigations)
        //                //        {
        //                //            GenerateNewMapping(foundNavigation, rootFromMapping, _currentPath.ToList());
        //                //        }
        //                //    }
        //                //}
        //                //else
        //                {
        //                    foreach (var fromMapping in navigationTreeNode.FromMappings)
        //                    {
        //                        GenerateNewMapping(navigationTreeNode, fromMapping, _currentPath.ToList());
        //                    }
        //                }
        //            }

        //            return extensionExpression;
        //        }

        //        return base.VisitExtension(extensionExpression);
        //    }

        //    private void GenerateNewMapping(NavigationTreeNode2 navigationTreeNode, List<string> currentMappingFragment, List<string> newMappingFragment)
        //    {
        //        var match = navigationTreeNode.FromMappings.Where(m => m.Count >= currentMappingFragment.Count && m.Take(currentMappingFragment.Count).SequenceEqual(currentMappingFragment)).Single();
        //        var newMapping = match.ToList();

        //        newMapping.RemoveRange(0, currentMappingFragment.Count);
        //        newMapping.InsertRange(0, newMappingFragment);

        //        if (!_newNavigationMappings.ContainsKey(navigationTreeNode))
        //        {
        //            _newNavigationMappings[navigationTreeNode] = new List<List<string>>();
        //        }

        //        _newNavigationMappings[navigationTreeNode].Add(newMapping);

        //        foreach (var child in navigationTreeNode.Children)
        //        {
        //            GenerateNewMapping(child, currentMappingFragment, newMappingFragment);
        //        }
        //    }
        //}

        private class PendingSelectorSourceMappingGenerator : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();
            private IEntityType _entityTypeOverride;

            public List<SourceMapping2> SourceMappings = new List<SourceMapping2>();

            public Dictionary<NavigationBindingExpression2, SourceMapping2> BindingToSourceMapping
                = new Dictionary<NavigationBindingExpression2, SourceMapping2>();

            public PendingSelectorSourceMappingGenerator(ParameterExpression rootParameter, IEntityType entityTypeOverride)
            {
                _rootParameter = rootParameter;
                _entityTypeOverride = entityTypeOverride;
            }

            // prune these nodes, we only want to look for entities accessible in the result
            protected override Expression VisitMember(MemberExpression memberExpression)
                => memberExpression;

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
                => methodCallExpression;

            // TODO: coalesce should pass? - TEST!
            protected override Expression VisitBinary(BinaryExpression binaryExpression)
                => binaryExpression;

            protected override Expression VisitUnary(UnaryExpression unaryExpression)
            {
                // TODO: handle cast here?

                return base.VisitUnary(unaryExpression);
            }

            protected override Expression VisitNew(NewExpression newExpression)
            {
                // TODO: when constructing a DTO, there will be arguments present, but no members - is it correct to just skip in this case?
                if (newExpression.Members != null)
                {
                    for (var i = 0; i < newExpression.Arguments.Count; i++)
                    {
                        _currentPath.Add(newExpression.Members[i].Name);
                        Visit(newExpression.Arguments[i]);
                        _currentPath.RemoveAt(_currentPath.Count - 1);
                    }
                }

                return newExpression;
            }

            protected override Expression VisitExtension(Expression extensionExpression)
            {
                if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
                {
                    if (navigationBindingExpression.RootParameter == _rootParameter)
                    {
                        var sourceMapping = new SourceMapping2
                        {
                            RootEntityType = _entityTypeOverride ?? navigationBindingExpression.EntityType,
                        };

                        var navigationTreeRoot = NavigationTreeNode2.CreateRoot(sourceMapping, _currentPath.ToList(), navigationBindingExpression.NavigationTreeNode.Optional);
                        sourceMapping.NavigationTree = navigationTreeRoot;

                        SourceMappings.Add(sourceMapping);
                        BindingToSourceMapping[navigationBindingExpression] = sourceMapping;
                    }

                    return extensionExpression;
                }

                return base.VisitExtension(extensionExpression);
            }
        }

        //private class EntityTypeAccessorMappingGenerator : ExpressionVisitor
        //{
        //    private ParameterExpression _rootParameter;
        //    private List<string> _currentPath = new List<string>();
        //    private IEntityType _entityTypeOverride;

        //    public EntityTypeAccessorMappingGenerator(ParameterExpression rootParameter, IEntityType entityTypeOverride)
        //    {
        //        _rootParameter = rootParameter;
        //        _entityTypeOverride = entityTypeOverride;
        //    }

        //    // prune these nodes, we only want to look for entities accessible in the result
        //    protected override Expression VisitMember(MemberExpression memberExpression)
        //        => memberExpression;

        //    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        //        => methodCallExpression;

        //    protected override Expression VisitBinary(BinaryExpression binaryExpression)
        //        => binaryExpression;

        //    protected override Expression VisitNew(NewExpression newExpression)
        //    {
        //        // TODO: when constructing a DTO, there will be arguments present, but no members - is it correct to just skip in this case?
        //        if (newExpression.Members != null)
        //        {
        //            for (var i = 0; i < newExpression.Arguments.Count; i++)
        //            {
        //                _currentPath.Add(newExpression.Members[i].Name);
        //                Visit(newExpression.Arguments[i]);
        //                _currentPath.RemoveAt(_currentPath.Count - 1);
        //            }
        //        }

        //        return newExpression;
        //    }

        //    protected override Expression VisitExtension(Expression extensionExpression)
        //    {
        //        if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
        //        {
        //            if (navigationBindingExpression.RootParameter == _rootParameter)
        //            {
        //                var sourceMapping = navigationBindingExpression.SourceMapping;
        //                sourceMapping.InitialPath = _currentPath.ToList();
        //                sourceMapping.RootEntityType = _entityTypeOverride ?? navigationBindingExpression.EntityType;
        //                sourceMapping.FoundNavigations = new List<NavigationTreeNode>();
        //                sourceMapping.TransparentIdentifierMapping = new List<(List<string> path, List<INavigation> navigations)>
        //                {
        //                    (path: new List<string>(), navigations: new List<INavigation>())
        //                };
        //            }

        //            return extensionExpression;
        //        }

        //        return base.VisitExtension(extensionExpression);
        //    }
        //}

        private class ExpressionReplacingVisitor : NavigationExpansionExpressionVisitorBase
        {
            private Expression _searchedFor;
            private Expression _replaceWith;

            public ExpressionReplacingVisitor(Expression searchedFor, Expression replaceWith)
            {
                _searchedFor = searchedFor;
                _replaceWith = replaceWith;
            }

            public override Expression Visit(Expression expression)
                => expression == _searchedFor
                ? _replaceWith
                : base.Visit(expression);
        }
    }
}
