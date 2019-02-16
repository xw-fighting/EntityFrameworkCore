// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
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

        //protected override Expression VisitExtension(Expression extensionExpression)
        //{
        //    if (extensionExpression is NullSafeEqualExpression nullSafeEqualExpression)
        //    {
        //        var newOuterKeyNullCheck = Visit(nullSafeEqualExpression.OuterKeyNullCheck);
        //        var newEqualExpression = (BinaryExpression)Visit(nullSafeEqualExpression.EqualExpression);

        //        if (newOuterKeyNullCheck != nullSafeEqualExpression.OuterKeyNullCheck
        //            || newEqualExpression != nullSafeEqualExpression.EqualExpression)
        //        {
        //            return new NullSafeEqualExpression(newOuterKeyNullCheck, newEqualExpression);
        //        }
        //    }

        //    return extensionExpression;
        //}

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
                case nameof(Queryable.Take):
                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                case nameof(Queryable.Any):
                case nameof(Queryable.Contains):
                //case nameof(Queryable.DefaultIfEmpty):
                    return ProcessTerminatingOperation2(methodCallExpression);

                case nameof(Queryable.Join):
                    return ProcessJoin(methodCallExpression);

                default:
                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
                    {
                        var result = ProcessSelectManyWithResultOperator(methodCallExpression);

                        return result;
                    }

                    return base.VisitMethodCall(methodCallExpression);
            }




            //        if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableWhereMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableWhereMethodInfo))
            //{
            //    var result = ProcessWhere(methodCallExpression);

            //    return result;
            //}

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableSelectMethodInfo))
            //{
            //    var result = ProcessSelect(methodCallExpression);

            //    return result;
            //}

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableOrderByMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOrderByDescendingMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableThenByMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableThenByDescendingMethodInfo))
            //{
            //    var result = ProcessOrderBy(methodCallExpression);

            //    return result;
            //}

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
            //{
            //    var result = ProcessSelectManyWithResultOperator(methodCallExpression);

            //    return result;
            //}

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableJoinMethodInfo))
            //{
            //    var result = ProcessJoin(methodCallExpression);

            //    return result;
            //}

            ////if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableGroupJoinMethodInfo))
            ////{
            ////    var result = ProcessGroupJoin(methodCallExpression);

            ////    return result;
            ////}

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)
            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains)

            //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType))
            //{
            //    var result = ProcessTerminatingOperation(methodCallExpression);

            //    return result;
            //}

            //return base.VisitMethodCall(methodCallExpression);
        }

        private NavigationExpansionExpressionState AdjustState(NavigationExpansionExpressionState state, NavigationExpansionExpression navigationExpansionExpression)
        {
            var currentParameter = state.CurrentParameter;
            state = navigationExpansionExpression.State;
            state.CurrentParameter = state.CurrentParameter ?? currentParameter;
            //state.PendingSelector = state.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            state.PendingSelector = state.PendingSelector ?? Expression.Lambda(state.CurrentParameter, state.CurrentParameter);
            state.PendingSelector2 = state.PendingSelector2 ?? Expression.Lambda(currentParameter, currentParameter);

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

                //// TODO: fix this!
                //var currentParameter = state.CurrentParameter;
                //state = navigationExpansionExpression.State;
                //state.CurrentParameter = state.CurrentParameter ?? currentParameter;
                //state.PendingSelector = state.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            //var combinedPredicate = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, predicate);

            //var binder = new NavigationPropertyBindingExpressionVisitor(
            //    state.CurrentParameter,
            //    state.SourceMappings);

            //var boundLambda = binder.Visit(combinedPredicate);

            var binder2 = new NavigationPropertyBindingExpressionVisitor2(
                predicate.Parameters[0],
                state.SourceMappings2);

            var boundLambda2 = binder2.Visit(predicate);

            //var cnrev = new CollectionNavigationRewritingExpressionVisitor2(state.CurrentParameter);
            //boundLambda = (LambdaExpression)cnrev.Visit(boundLambda);

            //combinedPredicate = (LambdaExpression)Visit(boundLambda);

            //combinedPredicate = (LambdaExpression)Visit(combinedPredicate);

            var result = FindAndApplyNavigations(source, predicate, state);

            //var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.state.CurrentParameter.Type);
            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.state.CurrentParameter.Type);
            var rewritten = Expression.Call(newMethodInfo, result.source, result.lambda);

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

            //var combinedSelector = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, selector.UnwrapQuote());
            //combinedSelector = (LambdaExpression)Visit(combinedSelector);

            //var binder = new NavigationPropertyBindingExpressionVisitor(
            //    state.CurrentParameter,
            //    state.SourceMappings);

            //var boundSelector = binder.Visit(combinedSelector);

            var binder2 = new NavigationPropertyBindingExpressionVisitor2(
                selectorParameter,
                state.SourceMappings2);

            var boundSelector2 = binder2.Visit(selector);

            //var cnrev = new CollectionNavigationRewritingExpressionVisitor2(state.CurrentParameter);
            //combinedSelector = (LambdaExpression)cnrev.Visit(boundSelector);

            var result = FindAndApplyNavigations(source, selector/*combinedSelector*/, state);
            result.state.PendingSelector = result.lambda;
            result.state.PendingSelector2 = (LambdaExpression)boundSelector2; // is this right??????

            var selectorRemapper = new FoundNavigationSelectorRemapper(selectorParameter);
            selectorRemapper.Remap(boundSelector2);

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

            //var combinedKeySelector = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, keySelector.UnwrapQuote());
            //combinedKeySelector = (LambdaExpression)Visit(combinedKeySelector);

            //var result = FindAndApplyNavigations(source, combinedKeySelector, state);
            var result = FindAndApplyNavigations(source, keySelector, state);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.state.CurrentParameter.Type, result.lambda.UnwrapQuote().Body.Type);
            var rewritten = Expression.Call(newMethodInfo, result.source, result.lambda);

            return new NavigationExpansionExpression(
                rewritten,
                result.state,
                methodCallExpression.Type);
        }

        private class CorrelationChecker : NavigationExpansionExpressionVisitorBase
        {
            private ParameterExpression _rootParameter;

            public bool Correlated { get; private set; } = false;

            public CorrelationChecker(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
            {
                if (parameterExpression == _rootParameter)
                {
                    Correlated = true;
                }

                return parameterExpression;
            }
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

                //var currentParameter = outerState.CurrentParameter;
                //outerState = outerNavigationExpansionExpression.State;
                //outerState.CurrentParameter = outerState.CurrentParameter ?? currentParameter;
                //outerState.PendingSelector = outerState.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            // remap inner selector in the context of the outer
            var collectionSelector = methodCallExpression.Arguments[1].UnwrapQuote();
            var combinedCollectionSelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, collectionSelector);

            var binder = new NavigationPropertyBindingExpressionVisitor(
                outerState.CurrentParameter,
                outerState.SourceMappings);

            var boundCollectionSelector = binder.Visit(combinedCollectionSelector);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(outerState.CurrentParameter);
            boundCollectionSelector = (LambdaExpression)cnrev.Visit(boundCollectionSelector);

            combinedCollectionSelector = (LambdaExpression)Visit(boundCollectionSelector);

            if (combinedCollectionSelector.Body is NavigationExpansionExpression collectionSelectorNavigationExpansionExpression)
            {
                var correlationChecker = new CorrelationChecker(combinedCollectionSelector.Parameters[0]);
                correlationChecker.Visit(collectionSelectorNavigationExpansionExpression);
                if (!correlationChecker.Correlated)
                {
                    // collection is uncorrelated with the source -> expand into subquery and keep as SelectMany
                    var newCollectionElementType = collectionSelectorNavigationExpansionExpression.Operand.Type.GetGenericArguments()[0];
                    var collectionSelectorNavigationExpansionExpressionOperand = collectionSelectorNavigationExpansionExpression.Operand;

                    var resultSelector = methodCallExpression.Arguments[2].UnwrapQuote();
                    var newResultCollectionParameter = Expression.Parameter(newCollectionElementType, resultSelector.Parameters[1].Name);

                    var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(outerState.CurrentParameter.Type, newCollectionElementType);

                    var transparentIdentifierCtorInfo
                        = resultType.GetTypeInfo().GetConstructors().Single();

                    var newResultSelector = Expression.Lambda(
                        Expression.New(transparentIdentifierCtorInfo, outerState.CurrentParameter, newResultCollectionParameter),
                        outerState.CurrentParameter,
                        newResultCollectionParameter);

                    var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                        outerState.CurrentParameter.Type,
                        newCollectionElementType,
                        newResultSelector.Body.Type);

                    // in case collection selector body is IQueryable, we need to adjust the type to IEnumerable, to match the SelectMany signature
                    // therefore the delegate type is specified explicitly
                    var newCollectionSelectorLambda = Expression.Lambda(
                        newMethodInfo.GetParameters()[1].ParameterType.GetGenericArguments()[0],
                        collectionSelectorNavigationExpansionExpressionOperand,
                        outerState.CurrentParameter);

                    var rewritten = Expression.Call(
                        newMethodInfo,
                        outerSource,
                        newCollectionSelectorLambda,
                        newResultSelector);

                    // this part is compied completely from join - DRYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY

                    var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");
                    var newNavigationExpansionMapping = new List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)>();

                    foreach (var outerMappingEntry in outerState.SourceMappings)
                    {
                        foreach (var outerTransparentIdentifierMapping in outerMappingEntry.TransparentIdentifierMapping)
                        {
                            outerTransparentIdentifierMapping.path.Insert(0, "Outer");
                        }
                    }

                    foreach (var innerMappingEntry in collectionSelectorNavigationExpansionExpression.State.SourceMappings)
                    {
                        foreach (var innerTransparentIdentifierMapping in innerMappingEntry.TransparentIdentifierMapping)
                        {
                            innerTransparentIdentifierMapping.path.Insert(0, "Inner");
                        }
                    }

                    var outerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Outer));
                    var innerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Inner));

                    var foo = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, resultSelector.Parameters[0]);
                    //var foo = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, newResultSelector.Parameters[0]);
                    var foo2 = ExpressionExtensions.CombineAndRemapLambdas(collectionSelectorNavigationExpansionExpression.State.PendingSelector, foo, resultSelector.Parameters[1]);

                    var foo3 = new ExpressionReplacingVisitor(outerState.CurrentParameter, outerAccess).Visit(foo2.Body);
                    //var foo4 = new ExpressionReplacingVisitor(collectionSelectorNavigationExpansionExpression.State.CurrentParameter, innerAccess).Visit(foo3);
                    var foo4 = new ExpressionReplacingVisitor(collectionSelectorNavigationExpansionExpression.State.CurrentParameter ?? resultSelector.Parameters[1], innerAccess).Visit(foo3);

                    // TODO: optimize out navigation expansions from the collection selector if the collection elements are not present in the final result?

                    var lambda = Expression.Lambda(foo4, transparentIdentifierParameter);

                    var select = QueryableSelectMethodInfo.MakeGenericMethod(transparentIdentifierParameter.Type, lambda.Body.Type);

                    var finalState = new NavigationExpansionExpressionState
                    {
                        PendingSelector = lambda,
                        CurrentParameter = transparentIdentifierParameter,
                        //FinalProjectionPath = new List<string>(),
                        SourceMappings = outerState.SourceMappings.Concat(collectionSelectorNavigationExpansionExpression.State.SourceMappings).ToList()
                    };

                    var fubar = new NavigationExpansionExpression(
                        rewritten,
                        finalState,
                        select.ReturnType);

                    var fubar22 = FindAndApplyNavigations(rewritten, lambda, finalState);
                    fubar22.state.PendingSelector = (LambdaExpression)fubar22.lambda;

                    return new NavigationExpansionExpression(
                        fubar22.source,
                        fubar22.state,
                        select.ReturnType);
                }
            }

            var result = FindAndApplyNavigations(outerSource, combinedCollectionSelector, outerState);

            outerSource = result.source;
            outerState = result.state;
            combinedCollectionSelector = result.lambda;

            var correlationPredicateExtractor = new CorrelationPredicateExtractingExpressionVisitor(combinedCollectionSelector.Parameters[0]);
            var collectionSelectorWithoutCorelationPredicate = correlationPredicateExtractor.Visit(combinedCollectionSelector);
            if (correlationPredicateExtractor.CorrelationPredicate != null)
            {
                var inner = (NavigationExpansionExpression)collectionSelectorWithoutCorelationPredicate.UnwrapQuote().Body;
                var remapResult = RemapTwoArgumentResultSelector(methodCallExpression.Arguments[2].UnwrapQuote(), outerState, inner.State);

                var joinMethodInfo = QueryableJoinMethodInfo.MakeGenericMethod(
                    outerState.CurrentParameter.Type,
                    inner.State.CurrentParameter.Type,
                    correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Left.Type,
                    remapResult.lambda.Body.Type);

                var outerKeyLambda = Expression.Lambda(correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Left, outerState.CurrentParameter);

                // inner key selecto needs to be remapped - when correlation predicate was being built it was based on "naked" entity from the collection
                // however there could have been navigations afterwards which would have caused types to be changed to TransparentIdentifiers
                // all necessary mappings should be stored in the inner NavigationExpansionExpression state.
                var innerKeyLambda = Expression.Lambda(correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Right, correlationPredicateExtractor.CorrelatedCollectionParameter);
                var combinedInnerKeyLambda = ExpressionExtensions.CombineAndRemapLambdas(inner.State.PendingSelector, innerKeyLambda);

                var rewritten = Expression.Call(
                    joinMethodInfo,
                    outerSource,
                    inner.Operand,
                    outerKeyLambda,
                    combinedInnerKeyLambda,
                    remapResult.lambda);

                var fubar = new NavigationExpansionExpression(
                    rewritten,
                    remapResult.state,
                    methodCallExpression.Type);

                return fubar;
            }

            return methodCallExpression;
        }

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

            var combinedOuterKeySelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, outerKeySelector);
            combinedOuterKeySelector = (LambdaExpression)Visit(combinedOuterKeySelector);

            var combinedInnerKeySelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, innerKeySelector);
            combinedInnerKeySelector = (LambdaExpression)Visit(combinedInnerKeySelector);

            var outerResult = FindAndApplyNavigations(outerSource, combinedOuterKeySelector, outerState);
            var innerResult = FindAndApplyNavigations(innerSource, combinedInnerKeySelector, innerState);

            var resultSelectorRemap = RemapTwoArgumentResultSelector(resultSelector, outerResult.state, innerResult.state);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                outerResult.state.CurrentParameter.Type,
                innerResult.state.CurrentParameter.Type,
                outerResult.lambda.UnwrapQuote().Body.Type,
                resultSelectorRemap.lambda.Body.Type);

            var rewritten = Expression.Call(
                newMethodInfo,
                outerResult.source,
                innerResult.source,
                outerResult.lambda,
                innerResult.lambda,
                resultSelectorRemap.lambda);

            var result = FindAndApplyNavigations(rewritten, resultSelectorRemap.state.PendingSelector, resultSelectorRemap.state);
            resultSelectorRemap.state.PendingSelector = result.lambda;

            return new NavigationExpansionExpression(
                result.source,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessGroupJoin(MethodCallExpression methodCallExpression)
        {
            return methodCallExpression;
        }

        private Expression ProcessTerminatingOperation2(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = Expression.Parameter(source.Type.GetGenericArguments()[0], source.Type.GetGenericArguments()[0].GenerateParameterName())
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;

                if (state.PendingSelector2 != null)
                {
                    var nrev = new NavigationReplacingExpressionVisitor2(
                        state.PendingSelector2.Parameters[0],
                        state.CurrentParameter);

                    var newSelector = nrev.Visit(state.PendingSelector2);

                    // TODO: test cases with casts, e.g. orders.Select(o => new { foo = (VipCustomer)o.Customer }).Distinct()
                    var entityTypeOverride = methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)
                        ? _model.FindEntityType(methodCallExpression.Method.GetGenericArguments()[0])
                        : null;

                    var pssmg = new PendingSelectorSourceMappingGenerator(state.PendingSelector2.Parameters[0], entityTypeOverride);
                    pssmg.Visit(state.PendingSelector2);

                    state.SourceMappings2 = pssmg.SourceMappings;

                    var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                        state.CurrentParameter.Type,
                        ((LambdaExpression)newSelector).Body.Type);

                    var result = Expression.Call(selectorMethodInfo, navigationExpansionExpression.Operand, newSelector);
                    state.PendingSelector = null;
                    state.PendingSelector2 = null;

                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)

                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType))
                    {
                        var newMethod = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                            result.Type.GetGenericArguments()[0]);

                        source = Expression.Call(newMethod, new[] { result });
                        //source = methodCallExpression.Update(methodCallExpression.Object, new[] { result });
                    }
                    else if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains))
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
                }

                state.CurrentParameter = null;

                // TODO: should we be reusing state?
                return new NavigationExpansionExpression(
                    source,
                    state,
                    methodCallExpression.Type);

            }

            // we should never hit this
            return methodCallExpression;
        }

        private Expression ProcessTerminatingOperation(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = Expression.Parameter(source.Type.GetGenericArguments()[0], source.Type.GetGenericArguments()[0].GenerateParameterName())
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;

                if (state.PendingSelector != null)
                {
                    var pendingSelectorParameter = state.PendingSelector.Parameters[0];

                    var binder = new NavigationPropertyBindingExpressionVisitor(
                        pendingSelectorParameter,
                        state.SourceMappings);

                    var boundSelector = binder.Visit(state.PendingSelector);

                    var nrev = new NavigationReplacingExpressionVisitor(
                        pendingSelectorParameter,
                        pendingSelectorParameter);

                    var newSelector = nrev.Visit(boundSelector);

                    var entityTypeOverride = methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)
                        ? _model.FindEntityType(methodCallExpression.Method.GetGenericArguments()[0])
                        : null;

                    var etamg = new EntityTypeAccessorMappingGenerator(pendingSelectorParameter, entityTypeOverride);
                    etamg.Visit(boundSelector);

                    var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                        pendingSelectorParameter.Type,
                        ((LambdaExpression)newSelector).Body.Type);

                    var result = Expression.Call(selectorMethodInfo, navigationExpansionExpression.Operand, newSelector);

                    state.PendingSelector = null;
                    //state.CurrentParameter = null;

                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)

                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType))
                    {
                        var newMethod = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                            result.Type.GetGenericArguments()[0]);

                        source = Expression.Call(newMethod, new[] { result });
                        //source = methodCallExpression.Update(methodCallExpression.Object, new[] { result });
                    }
                    else if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains))
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
                }

                state.CurrentParameter = null;

                // TODO: should we be reusing state?
                return new NavigationExpansionExpression(
                    source,
                    state,
                    methodCallExpression.Type);
            }

            // we should never hit this

            return methodCallExpression;
        }

        private Expression ProcessOfType(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = Expression.Parameter(source.Type.GetGenericArguments()[0], source.Type.GetGenericArguments()[0].GenerateParameterName())
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            if (state.PendingSelector != null)
            {
                var pendingSelectorParameter = state.PendingSelector.Parameters[0];

                var binder = new NavigationPropertyBindingExpressionVisitor(
                    pendingSelectorParameter,
                    state.SourceMappings);

                var boundSelector = binder.Visit(state.PendingSelector);

                var nrev = new NavigationReplacingExpressionVisitor(
                    pendingSelectorParameter,
                    pendingSelectorParameter);

                var newSelector = nrev.Visit(boundSelector);

                // TODO: handle owned type case - it should always be a noop since we don't support owned type inheritance on the model level
                var newEntityType = _model.FindEntityType(methodCallExpression.Method.GetGenericArguments()[0]);

                var etamg = new EntityTypeAccessorMappingGenerator(pendingSelectorParameter, newEntityType);
                etamg.Visit(boundSelector);

                var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                    pendingSelectorParameter.Type,
                    ((LambdaExpression)newSelector).Body.Type);

                var result = Expression.Call(selectorMethodInfo, /*navigationExpansionExpression.Operand*/source, newSelector);

                state.PendingSelector = null;
                state.CurrentParameter = null;
            }

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

                var result = new NavigationExpansionExpression(
                    constantExpression,
                    new NavigationExpansionExpressionState
                    {
                        SourceMappings = new List<SourceMapping>
                        {
                            new SourceMapping
                            {
                                RootEntityType = entityType,
                                TransparentIdentifierMapping = new List<(List<string> path, List<INavigation> navigations)>
                                {
                                    (path: new List<string>(), navigations: new List<INavigation>())
                                }
                            }
                        },

                        SourceMappings2 = new List<SourceMapping2> { sourceMapping },
                        PendingSelector2 = Expression.Lambda(pendingSelectorParameter, pendingSelectorParameter),
                    },
                    constantExpression.Type);

                return result;
            }

            return base.VisitConstant(constantExpression);
        }

        private (Expression source, LambdaExpression lambda, NavigationExpansionExpressionState state) FindAndApplyNavigations(
            Expression source,
            LambdaExpression lambda,
            NavigationExpansionExpressionState state)
        {
            //var binder = new NavigationPropertyBindingExpressionVisitor(
            //    state.CurrentParameter,
            //    state.SourceMappings);

            //var boundLambda = binder.Visit(lambda);

            var binder2 = new NavigationPropertyBindingExpressionVisitor2(
                lambda.Parameters[0],
                state.SourceMappings2);

            var boundLambda2 = binder2.Visit(lambda);

            //var cnrev = new CollectionNavigationRewritingExpressionVisitor2(state.CurrentParameter);
            //boundLambda = (LambdaExpression)cnrev.Visit(boundLambda);

            //var nfev = new NavigationFindingExpressionVisitor(state.CurrentParameter);
            //nfev.Visit(boundLambda);

            var result = (source, parameter: state.CurrentParameter, pendingSelector: state.PendingSelector);
            foreach (var sourceMapping in state.SourceMappings2)
            {
                if (sourceMapping.NavigationTree.Flatten().Any(n => !n.Expanded))
                {
                    foreach (var navigationTree in sourceMapping.NavigationTree.Children)
                    {
                        result = AddNavigationJoin2(
                            result.source,
                            result.parameter,
                            sourceMapping,
                            state.SourceMappings2,
                            navigationTree,
                            new List<INavigation>(),
                            result.pendingSelector);
                    }
                }
            }

            var nrev2 = new NavigationReplacingExpressionVisitor2(
                lambda.Parameters[0],
                result.parameter);

            var newLambda2 = (LambdaExpression)nrev2.Visit(boundLambda2);

            var newState = new NavigationExpansionExpressionState
            {
                CurrentParameter = result.parameter,
                SourceMappings = state.SourceMappings,
                SourceMappings2 = state.SourceMappings2,
                PendingSelector = result.pendingSelector,
            };

            // TODO: improve this (maybe a helper method?)
            if (source.Type.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)
                && result.source.Type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                var toOrderedMethod = typeof(NavigationExpansionExpression).GetMethod(nameof(NavigationExpansionExpression.ToOrdered)).MakeGenericMethod(result.source.Type.GetGenericArguments()[0]);
                var toOrderedCall = Expression.Call(toOrderedMethod, result.source);

                return (source: toOrderedCall, lambda: newLambda2, state: newState);
            }

            return (result.source, lambda: newLambda2, state: newState);
        }

        private (Expression source, ParameterExpression parameter, LambdaExpression pendingSelector) AddNavigationJoin(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            SourceMapping sourceMapping,
            SourceMapping2 sourceMapping2,
            List<SourceMapping> allSourceMappings,
            NavigationTreeNode navigationTree,
            NavigationTreeNode2 navigationTree2,
            List<INavigation> navigationPath,
            LambdaExpression pendingSelector)
        {
            var path = navigationTree2.GeneratePath();
            if (!navigationTree2.Expanded)
            //if (!sourceMapping.TransparentIdentifierMapping.Any(m => m.navigations.Count == path.Count && m.navigations.Zip(path, (o, i) => o.Name == i).All(r => r)))
            {
                var navigation = navigationTree.Navigation;
                var sourceType = sourceExpression.Type.GetGenericArguments()[0];
                var navigationTargetEntityType = navigation.GetTargetType();

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var transparentIdentifierAccessorPath = sourceMapping.TransparentIdentifierMapping.Where(
                    m => m.navigations.Count == navigationPath.Count
                        && m.navigations.Zip(navigationPath, (o, i) => o == i).All(r => r)).SingleOrDefault().path;

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, sourceMapping.InitialPath, transparentIdentifierAccessorPath);

                var outerKeySelectorBody = CreateKeyAccessExpression(
                    transparentIdentifierAccessorExpression,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.Properties
                        : navigation.ForeignKey.PrincipalKey.Properties,
                    addNullCheck: navigationTree.Parent != null && navigationTree.Parent.Optional);

                var innerKeySelectorParameterType = navigationTargetEntityType.ClrType;
                var innerKeySelectorParameter = Expression.Parameter(
                    innerKeySelectorParameterType,
                    parameterExpression.Name + "." + navigationTree.Navigation.Name);

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
                if (navigationTree.Optional)
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

                //sourceMapping2.RootToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                //if (navigationTree.Optional)
                //{
                //    sourceMapping2.RootToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                //}

                //RemapNavigationChain(navigationTree2.Parent, navigationTree.Optional);

                //// TODO: there should be only one mapping for the current navigation at this point, looping through "just in case"
                //navigationTree2.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));

                // TODO: do we need to add the empty entry to ALL source mappings or just the one that is being processed?
                if (navigationPath.Count == 0
                    && !sourceMapping.TransparentIdentifierMapping.Any(m => m.navigations.Count == 0))
                {
                    sourceMapping.TransparentIdentifierMapping.Add((path: new List<string>(), navigations: navigationPath.ToList()));
                }

                foreach (var aSourceMapping in allSourceMappings)
                {
                    foreach (var transparentIdentifierMappingElement in aSourceMapping.TransparentIdentifierMapping)
                    {
                        transparentIdentifierMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));

                        // in case of GroupJoin (optional navigation) source is hidden deeper since we also project the grouping
                        if (navigationTree.Optional)
                        {
                            transparentIdentifierMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        }
                    }
                }

                navigationPath.Add(navigation);

                foreach (var aSourceMapping in allSourceMappings)
                {
                    aSourceMapping.TransparentIdentifierMapping.Add((path: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }, navigations: navigationPath.ToList()));
                }

                if (pendingSelector != null)
                {
                    var psuev = new PendingSelectorUpdatingExpressionVisitor(oldParameterExpression, parameterExpression, navigationTree.Optional);
                    pendingSelector = (LambdaExpression)psuev.Visit(pendingSelector);
                }

                navigationTree.Expanded = true;
            }
            else
            {
                navigationPath.Add(navigationTree.Navigation);
            }

            var result = (source: sourceExpression, parameter: parameterExpression, pendingSelector);

            if (navigationTree.Children.Count != navigationTree2.Children.Count)
            {
                throw new InvalidOperationException("navigation tree children count mismatch!");
            }

            for (int i = 0; i < navigationTree2.Children.Count; i++)
            {
                result = AddNavigationJoin(
                    result.source,
                    result.parameter,
                    sourceMapping,
                    sourceMapping2,
                    allSourceMappings,
                    navigationTree.Children[i],
                    navigationTree2.Children[i],
                    navigationPath.ToList(),
                    result.pendingSelector);
            }

            //foreach (var child in navigationTree.Children)
            //{
            //    result = AddNavigationJoin(
            //        result.source,
            //        result.parameter,
            //        sourceMapping,
            //        allSourceMappings,
            //        child,
            //        navigationPath.ToList(),
            //        result.pendingSelector);
            //}

            return result;
        }

        private (Expression source, ParameterExpression parameter, LambdaExpression pendingSelector) AddNavigationJoin2(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            SourceMapping2 sourceMapping2,
            List<SourceMapping2> allSourceMappings,
            NavigationTreeNode2 navigationTree2,
            List<INavigation> navigationPath,
            LambdaExpression pendingSelector)
        {
            if (!navigationTree2.Expanded)
            {
                var navigation = navigationTree2.Navigation;
                var sourceType = sourceExpression.Type.GetGenericArguments()[0];
                var navigationTargetEntityType = navigation.GetTargetType();

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var transparentIdentifierAccessorPath = navigationTree2.Parent.ToMapping;

                //var transparentIdentifierAccessorPath = sourceMapping.TransparentIdentifierMapping.Where(
                //    m => m.navigations.Count == navigationPath.Count
                //        && m.navigations.Zip(navigationPath, (o, i) => o == i).All(r => r)).SingleOrDefault().path;

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, null, transparentIdentifierAccessorPath);

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

                //// TODO: simplify, could probably do this in one step since root is part of the same tree structure now
                //sourceMapping2.NavigationTree.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                //if (navigationTree2.Optional)
                //{
                //    sourceMapping2.NavigationTree.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                //}

                // remap navigation 'To' paths -> for this navigation prepend "Inner", for every other (already expanded) navigation prepend "Outer"
                navigationTree2.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                foreach (var sourceMapping in allSourceMappings)
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

                //RemapNavigationChain(navigationTree2.Parent, navigationTree2.Optional);
                navigationTree2.Expanded = true;

                navigationPath.Add(navigation);

                //foreach (var aSourceMapping in allSourceMappings)
                //{
                //    aSourceMapping.TransparentIdentifierMapping.Add((path: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }, navigations: navigationPath.ToList()));
                //}

                if (pendingSelector != null)
                {
                    var psuev = new PendingSelectorUpdatingExpressionVisitor(oldParameterExpression, parameterExpression, navigationTree2.Optional);
                    pendingSelector = (LambdaExpression)psuev.Visit(pendingSelector);
                }

            }
            else
            {
                navigationPath.Add(navigationTree2.Navigation);
            }

            var result = (source: sourceExpression, parameter: parameterExpression, pendingSelector);

            foreach (var child in navigationTree2.Children)
            {
                result = AddNavigationJoin2(
                    result.source,
                    result.parameter,
                    sourceMapping2,
                    allSourceMappings,
                    child,
                    navigationPath.ToList(),
                    result.pendingSelector);
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
            var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(outerState.CurrentParameter.Type, innerState.CurrentParameter.Type);

            var transparentIdentifierCtorInfo
                = resultType.GetTypeInfo().GetConstructors().Single();

            var lambda = Expression.Lambda(
                Expression.New(transparentIdentifierCtorInfo, outerState.CurrentParameter, innerState.CurrentParameter),
                outerState.CurrentParameter,
                innerState.CurrentParameter);

            var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");
            var newNavigationExpansionMapping = new List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)>();

            foreach (var outerMappingEntry in outerState.SourceMappings)
            {
                foreach (var outerTransparentIdentifierMapping in outerMappingEntry.TransparentIdentifierMapping)
                {
                    outerTransparentIdentifierMapping.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                }
            }

            foreach (var innerMappingEntry in innerState.SourceMappings)
            {
                foreach (var innerTransparentIdentifierMapping in innerMappingEntry.TransparentIdentifierMapping)
                {
                    innerTransparentIdentifierMapping.path.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                }
            }

            var outerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Outer));
            var innerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Inner));

            resultSelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, resultSelector.Parameters[0]);
            resultSelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, resultSelector, resultSelector.Parameters[1]);

            var resultSelectorBody = new ExpressionReplacingVisitor(outerState.CurrentParameter, outerAccess).Visit(resultSelector.Body);
            resultSelectorBody = new ExpressionReplacingVisitor(innerState.CurrentParameter, innerAccess).Visit(resultSelectorBody);

            var pendingSelector = Expression.Lambda(resultSelectorBody, transparentIdentifierParameter);
            var select = QueryableSelectMethodInfo.MakeGenericMethod(transparentIdentifierParameter.Type, pendingSelector.Body.Type);

            var state = new NavigationExpansionExpressionState
            {
                PendingSelector = pendingSelector,
                CurrentParameter = transparentIdentifierParameter,
                //FinalProjectionPath = new List<string>(),
                SourceMappings = outerState.SourceMappings.Concat(innerState.SourceMappings).ToList()
            };

            return (lambda, state);
        }

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

        private class PendingSelectorUpdatingExpressionVisitor : ExpressionVisitor
        {
            private ParameterExpression _oldParameter;
            private ParameterExpression _newParameter;
            private bool _optional;

            public PendingSelectorUpdatingExpressionVisitor(ParameterExpression oldParameter, ParameterExpression newParameter, bool optional)
            {
                _oldParameter = oldParameter;
                _newParameter = newParameter;
                _optional = optional;
            }

            protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
            {
                // TODO: combine this with navigation replacing expression visitor? logic is the same
                var newParameters = new List<ParameterExpression>();
                var parameterChanged = false;

                foreach (var parameter in lambdaExpression.Parameters)
                {
                    if (parameter == _oldParameter)
                    {
                        newParameters.Add(_newParameter);
                        parameterChanged = true;
                    }
                    else
                    {
                        newParameters.Add(parameter);
                    }
                }

                var newBody = Visit(lambdaExpression.Body);

                return parameterChanged || newBody != lambdaExpression.Body
                    ? Expression.Lambda(newBody, newParameters)
                    : lambdaExpression;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
                => parameterExpression == _oldParameter
                ? _optional
                    ? Expression.Field(
                        Expression.Field(
                            _newParameter,
                            "Outer"),
                        "Outer")
                    : Expression.Field(
                        _newParameter,
                        "Outer")
                : (Expression)parameterExpression;
        }



        private class FoundNavigationSelectorRemapper : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();

            private Dictionary<NavigationTreeNode2, List<List<string>>> _newNavigationMappings;

            public FoundNavigationSelectorRemapper(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
            }

            public void Remap(Expression expression)
            {
                _newNavigationMappings = new Dictionary<NavigationTreeNode2, List<List<string>>>();

                Visit(expression);

                // TODO: clear old mappings, some navigations may/should become inaccessible
                foreach (var mappingElement in _newNavigationMappings)
                {
                    mappingElement.Key.FromMappings = mappingElement.Value;
                }
            }

            // prune these nodes, we only want to look for entities accessible in the result
            protected override Expression VisitMember(MemberExpression memberExpression)
                => memberExpression;

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
                => methodCallExpression;

            protected override Expression VisitBinary(BinaryExpression binaryExpression)
                => binaryExpression;

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
                        var navigationTreeNode = navigationBindingExpression.NavigationTreeNode;
                        //if (navigationTreeNode == null)
                        //{
                        //    // TODO: DRY this - make source mapping a NavigationTreeNode without Navigation property
                        //    if (!_newRootMappings.ContainsKey(navigationBindingExpression.SourceMapping))
                        //    {
                        //        _newRootMappings[navigationBindingExpression.SourceMapping] = new List<List<string>>();
                        //    }

                        //    _newRootMappings[navigationBindingExpression.SourceMapping].Add(_currentPath.ToList());

                        //    foreach (var rootFromMapping in navigationBindingExpression.SourceMapping.RootFromMappings)
                        //    {
                        //        foreach (var foundNavigation in navigationBindingExpression.SourceMapping.FoundNavigations)
                        //        {
                        //            GenerateNewMapping(foundNavigation, rootFromMapping, _currentPath.ToList());
                        //        }
                        //    }
                        //}
                        //else
                        {
                            foreach (var fromMapping in navigationTreeNode.FromMappings)
                            {
                                GenerateNewMapping(navigationTreeNode, fromMapping, _currentPath.ToList());
                            }
                        }
                    }

                    return extensionExpression;
                }

                return base.VisitExtension(extensionExpression);
            }

            private void GenerateNewMapping(NavigationTreeNode2 navigationTreeNode, List<string> currentMappingFragment, List<string> newMappingFragment)
            {
                var match = navigationTreeNode.FromMappings.Where(m => m.Count >= currentMappingFragment.Count && m.Take(currentMappingFragment.Count).SequenceEqual(currentMappingFragment)).Single();
                var newMapping = match.ToList();

                newMapping.RemoveRange(0, currentMappingFragment.Count);
                newMapping.InsertRange(0, newMappingFragment);

                if (!_newNavigationMappings.ContainsKey(navigationTreeNode))
                {
                    _newNavigationMappings[navigationTreeNode] = new List<List<string>>();
                }

                _newNavigationMappings[navigationTreeNode].Add(newMapping);

                foreach (var child in navigationTreeNode.Children)
                {
                    GenerateNewMapping(child, currentMappingFragment, newMappingFragment);
                }
            }
        }

        private class PendingSelectorSourceMappingGenerator : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();
            private IEntityType _entityTypeOverride;

            public List<SourceMapping2> SourceMappings = new List<SourceMapping2>();

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
                    }

                    return extensionExpression;
                }

                return base.VisitExtension(extensionExpression);
            }
        }

        private class EntityTypeAccessorMappingGenerator : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();
            private IEntityType _entityTypeOverride;

            public EntityTypeAccessorMappingGenerator(ParameterExpression rootParameter, IEntityType entityTypeOverride)
            {
                _rootParameter = rootParameter;
                _entityTypeOverride = entityTypeOverride;
            }

            // prune these nodes, we only want to look for entities accessible in the result
            protected override Expression VisitMember(MemberExpression memberExpression)
                => memberExpression;

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
                => methodCallExpression;

            protected override Expression VisitBinary(BinaryExpression binaryExpression)
                => binaryExpression;

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
                if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
                {
                    if (navigationBindingExpression.RootParameter == _rootParameter)
                    {
                        var sourceMapping = navigationBindingExpression.SourceMapping;
                        sourceMapping.InitialPath = _currentPath.ToList();
                        sourceMapping.RootEntityType = _entityTypeOverride ?? navigationBindingExpression.EntityType;
                        sourceMapping.FoundNavigations = new List<NavigationTreeNode>();
                        sourceMapping.TransparentIdentifierMapping = new List<(List<string> path, List<INavigation> navigations)>
                        {
                            (path: new List<string>(), navigations: new List<INavigation>())
                        };
                    }

                    return extensionExpression;
                }

                return base.VisitExtension(extensionExpression);
            }
        }

        private class ExpressionReplacingVisitor : ExpressionVisitor
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
