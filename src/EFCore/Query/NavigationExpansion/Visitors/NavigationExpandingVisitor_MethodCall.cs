// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Extensions.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public partial class NavigationExpandingVisitor : LinqQueryVisitorBase
    {
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

                case nameof(Queryable.DefaultIfEmpty):
                    return ProcessDefaultIfEmpty(methodCallExpression);

                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                    return ProcessCardinalityReducingOperation(methodCallExpression);

                case nameof(Queryable.Distinct):
                case nameof(Queryable.Skip):
                case nameof(Queryable.Take):
                case nameof(Queryable.Any):
                case nameof(Queryable.OfType):
                case "AsTracking":
                case "AsNoTracking":
                    return ProcessTerminatingOperation(methodCallExpression);

                case nameof(Queryable.Join):
                    return ProcessJoin(methodCallExpression);

                case nameof(Queryable.GroupJoin):
                    return ProcessGroupJoin(methodCallExpression);

                case nameof(Queryable.SelectMany):
                    return ProcessSelectMany(methodCallExpression);

                case nameof(Queryable.Contains):
                    return ProcessContains(methodCallExpression);

                case "MaterializeCollectionNavigation":
                    var newArgument = (NavigationExpansionExpression)Visit(methodCallExpression.Arguments[1]);
                    return new NavigationExpansionExpression(newArgument, newArgument.State, methodCallExpression.Type);

                default:
                    return base.VisitMethodCall(methodCallExpression);
            }
        }

        private Expression VisitSourceExpression(Expression sourceExpression)
        {
            var result = Visit(sourceExpression);

            return result;
            //return result is CustomRootExpression2 customRootExpression2
            //    ? customRootExpression2.Unwrap()
            //    : result;
        }

        private NavigationExpansionExpressionState AdjustState(
            NavigationExpansionExpressionState state,
            NavigationExpansionExpression navigationExpansionExpression)
        {
            var currentParameter = state.CurrentParameter;
            state = navigationExpansionExpression.State;

            if (state.CurrentParameter.Name == null
                && state.CurrentParameter.Name != currentParameter.Name)
            {
                AdjustCurrentParameterName(state, currentParameter.Name);
            }

            return state;
        }

        private NavigationExpansionExpressionState AdjustCurrentParameterName(
            NavigationExpansionExpressionState state,
            string newParameterName)
        {
            if (newParameterName == null || newParameterName == state.CurrentParameter.Name)
            {
                return state;
            }

            var newParameter = Expression.Parameter(state.CurrentParameter.Type, newParameterName);
            state.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(state.CurrentParameter, newParameter).Visit(state.PendingSelector);
            state.CurrentParameter = newParameter;

            return state;
        }

        private Expression ProcessWhere(MethodCallExpression methodCallExpression)
        {
            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var predicate = methodCallExpression.Arguments[1].UnwrapQuote();
            var state = new NavigationExpansionExpressionState(predicate.Parameters[0]);

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            var result = FindAndApplyNavigations(source, predicate, state);
            var newPredicateBody = new NavigationPropertyUnbindingVisitor(result.state.CurrentParameter).Visit(result.lambdaBody);

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
            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var selector = methodCallExpression.Arguments[1].UnwrapQuote();
            var selectorParameter = selector.Parameters[0];
            var state = new NavigationExpansionExpressionState(selectorParameter);

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            return ProcessSelectCore(source, state, selector, methodCallExpression.Type);
        }

        private Expression ProcessSelectCore(Expression source, NavigationExpansionExpressionState state, LambdaExpression selector, Type resultType)
        {
            var result = FindAndApplyNavigations(source, selector, state);
            result.state.PendingSelector = Expression.Lambda(result.lambdaBody, result.state.CurrentParameter);

            // TODO: unless it's identity projection
            result.state.ApplyPendingSelector = true;

            return new NavigationExpansionExpression(
                result.source,
                result.state,
                resultType/*methodCallExpression.Type*/);
        }

        private Expression ProcessOrderBy(MethodCallExpression methodCallExpression)
        {
            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var keySelector = methodCallExpression.Arguments[1].UnwrapQuote();
            var state = new NavigationExpansionExpressionState(keySelector.Parameters[0]);

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }

            var result = FindAndApplyNavigations(source, keySelector, state);
            var newKeySelectorBody = new NavigationPropertyUnbindingVisitor(result.state.CurrentParameter).Visit(result.lambdaBody);

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

        private Expression ProcessDefaultIfEmpty(MethodCallExpression methodCallExpression)
        {
            // TODO: clean this up, i.e. in top level switch statement pick method based on method info, not only the name
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDefaultIfEmptyWithDefaultValue))
            {
                return ProcessTerminatingOperation(methodCallExpression);
            }

            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState(Expression.Parameter(source.Type.GetSequenceType()));
            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                state = AdjustState(state, navigationExpansionExpression);
            }
            else
            {
                throw new InvalidOperationException("not handling nav expansion correctly");
            }

            foreach (var sourceMapping in state.SourceMappings)
            {
                sourceMapping.NavigationTree.MakeOptional();
            }

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(state.CurrentParameter.Type);
            var rewritten = Expression.Call(
                newMethodInfo,
                source);

            return new NavigationExpansionExpression(
                rewritten,
                state,
                methodCallExpression.Type);
        }

        private Expression ProcessSelectMany(MethodCallExpression methodCallExpression)
        {
            var outerSource = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var outerState = new NavigationExpansionExpressionState(Expression.Parameter(methodCallExpression.Method.GetGenericArguments()[0]));

            if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            {
                outerSource = outerNavigationExpansionExpression.Operand;
                outerState = AdjustState(outerState, outerNavigationExpansionExpression);
            }

            var collectionSelector = methodCallExpression.Arguments[1].UnwrapQuote();

            AdjustCurrentParameterName(outerState, collectionSelector.Parameters[0].Name);

            var collectionSelectorResult = FindAndApplyNavigations(outerSource, collectionSelector, outerState);
            outerState = collectionSelectorResult.state;
            outerSource = collectionSelectorResult.source;

            var collectionSelectorNavigationExpansionExpression = collectionSelectorResult.lambdaBody as NavigationExpansionExpression;
            //?? (collectionSelectorResult.lambdaBody as CustomRootExpression2)?.Unwrap() as NavigationExpansionExpression;

            if (collectionSelectorNavigationExpansionExpression != null)
            //if (collectionSelectorResult.lambdaBody is NavigationExpansionExpression collectionSelectorNavigationExpansionExpression)
            {
                var collectionSelectorState = collectionSelectorNavigationExpansionExpression.State;
                var collectionSelectorLambdaBody = collectionSelectorNavigationExpansionExpression.Operand;

                if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
                {
                    // TODO: && or || ??
                    if (outerState.CurrentParameter.Name == null
                        && outerState.CurrentParameter.Name != methodCallExpression.Arguments[2].UnwrapQuote().Parameters[0].Name)
                    {
                        var newOuterParameter = Expression.Parameter(outerState.CurrentParameter.Type, methodCallExpression.Arguments[2].UnwrapQuote().Parameters[0].Name);
                        outerState.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(outerState.CurrentParameter, newOuterParameter).Visit(outerState.PendingSelector);
                        collectionSelectorLambdaBody = new ExpressionReplacingVisitor(outerState.CurrentParameter, newOuterParameter).Visit(collectionSelectorLambdaBody);
                        outerState.CurrentParameter = newOuterParameter;
                    }

                    if (collectionSelectorState.CurrentParameter.Name == null
                        && collectionSelectorState.CurrentParameter.Name != methodCallExpression.Arguments[2].UnwrapQuote().Parameters[1].Name)
                    {
                        var newInnerParameter = Expression.Parameter(collectionSelectorState.CurrentParameter.Type, methodCallExpression.Arguments[2].UnwrapQuote().Parameters[1].Name);
                        collectionSelectorState.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(collectionSelectorState.CurrentParameter, newInnerParameter).Visit(collectionSelectorState.PendingSelector);
                        collectionSelectorState.CurrentParameter = newInnerParameter;
                    }
                }

                if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo)
                    && (collectionSelectorState.CurrentParameter.Name == null
                        || collectionSelectorState.CurrentParameter.Name != methodCallExpression.Arguments[2].UnwrapQuote().Parameters[1].Name))
                {
                    // TODO: should we rename the second parameter according to the second parameter of the result selector instead?
                    var newParameter = Expression.Parameter(collectionSelectorState.CurrentParameter.Type, methodCallExpression.Arguments[2].UnwrapQuote().Parameters[1].Name);
                    collectionSelectorState.PendingSelector = (LambdaExpression)new ExpressionReplacingVisitor(collectionSelectorState.CurrentParameter, newParameter).Visit(collectionSelectorState.PendingSelector);
                    collectionSelectorState.CurrentParameter = newParameter;
                }

                // in case collection selector body is IQueryable, we need to adjust the type to IEnumerable, to match the SelectMany signature
                // therefore the delegate type is specified explicitly
                var collectionSelectorLambdaType = typeof(Func<,>).MakeGenericType(
                    outerState.CurrentParameter.Type,
                    typeof(IEnumerable<>).MakeGenericType(collectionSelectorNavigationExpansionExpression.State.CurrentParameter.Type));

                var newCollectionSelectorLambda = Expression.Lambda(
                    collectionSelectorLambdaType,
                    collectionSelectorLambdaBody,
                    outerState.CurrentParameter);

                newCollectionSelectorLambda = (LambdaExpression)new NavigationPropertyUnbindingVisitor(outerState.CurrentParameter).Visit(newCollectionSelectorLambda);

                if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyMethodInfo))
                {
                    return BuildSelectManyWithoutResultOperatorMethodCall(methodCallExpression, outerSource, outerState, newCollectionSelectorLambda, collectionSelectorState);
                }

                var resultSelector = methodCallExpression.Arguments[2].UnwrapQuote();
                var resultSelectorRemap = RemapTwoArgumentResultSelector(resultSelector, outerState, collectionSelectorNavigationExpansionExpression.State);

                var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                    outerState.CurrentParameter.Type,
                    collectionSelectorState.CurrentParameter.Type,
                    resultSelectorRemap.lambda.Body.Type);

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

            throw new InvalidOperationException("collection selector was not NavigationExpansionExpression");
        }

        private Expression BuildSelectManyWithoutResultOperatorMethodCall(
            MethodCallExpression methodCallExpression,
            Expression outerSource,
            NavigationExpansionExpressionState outerState,
            LambdaExpression newCollectionSelectorLambda,
            NavigationExpansionExpressionState collectionSelectorState)
        {
            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                outerState.CurrentParameter.Type,
                collectionSelectorState.CurrentParameter.Type);

            var rewritten = Expression.Call(
                newMethodInfo,
                outerSource,
                newCollectionSelectorLambda);

            return new NavigationExpansionExpression(
                rewritten,
                collectionSelectorState,
                methodCallExpression.Type);
        }

        private Expression ProcessJoin(MethodCallExpression methodCallExpression)
        {
            // TODO: move this to the big switch/case - this is for the string.Join case which would go here since it's matched by name atm
            if (!methodCallExpression.Method.MethodIsClosedFormOf(QueryableJoinMethodInfo))
            {
                return base.VisitMethodCall(methodCallExpression);
            }

            var outerSource = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var innerSource = VisitSourceExpression(methodCallExpression.Arguments[1]);

            var outerKeySelector = methodCallExpression.Arguments[2].UnwrapQuote();
            var innerKeySelector = methodCallExpression.Arguments[3].UnwrapQuote();
            var resultSelector = methodCallExpression.Arguments[4].UnwrapQuote();

            var outerState = new NavigationExpansionExpressionState(outerKeySelector.Parameters[0]);
            var innerState = new NavigationExpansionExpressionState(innerKeySelector.Parameters[0]);

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

            var newOuterKeySelectorBody = new NavigationPropertyUnbindingVisitor(outerResult.state.CurrentParameter).Visit(outerResult.lambdaBody);
            var newInnerKeySelectorBody = new NavigationPropertyUnbindingVisitor(innerResult.state.CurrentParameter).Visit(innerResult.lambdaBody);

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
            throw new NotSupportedException("NavRewrite doesn't support GroupJoin");
        }

        //private Expression ProcessGroupJoin(MethodCallExpression methodCallExpression)
        //{
        //    var outerSource = VisitSourceExpression(methodCallExpression.Arguments[0]);
        //    var innerSource = VisitSourceExpression(methodCallExpression.Arguments[1]);

        //    var outerKeySelector = methodCallExpression.Arguments[2].UnwrapQuote();
        //    var innerKeySelector = methodCallExpression.Arguments[3].UnwrapQuote();
        //    var resultSelector = methodCallExpression.Arguments[4].UnwrapQuote();

        //    var outerState = new NavigationExpansionExpressionState(outerKeySelector.Parameters[0]);
        //    var innerState = new NavigationExpansionExpressionState(innerKeySelector.Parameters[0]);

        //    if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
        //    {
        //        outerSource = outerNavigationExpansionExpression.Operand;
        //        outerState = AdjustState(outerState, outerNavigationExpansionExpression);
        //    }

        //    if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
        //    {
        //        innerSource = innerNavigationExpansionExpression.Operand;
        //        innerState = AdjustState(innerState, innerNavigationExpansionExpression);
        //    }

        //    var outerResult = FindAndApplyNavigations(outerSource, outerKeySelector, outerState);
        //    var innerResult = FindAndApplyNavigations(innerSource, innerKeySelector, innerState);

        //    var newOuterKeySelectorBody = new NavigationPropertyUnbindingVisitor(outerResult.state.CurrentParameter).Visit(outerResult.lambdaBody);
        //    var newInnerKeySelectorBody = new NavigationPropertyUnbindingVisitor(innerResult.state.CurrentParameter).Visit(innerResult.lambdaBody);

        //    //-----------------------------------------------------------------------------------------------------------------------------------------------------

        //    var resultSelectorBody = resultSelector.Body;
        //    var remappedResultSelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, resultSelector.Parameters[0]);

        //    var groupingParameter = resultSelector.Parameters[1];
        //    var newGroupingParameter = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(innerState.CurrentParameter.Type), "new_" + groupingParameter.Name);

        //    var groupingMapping = new List<string> { nameof(TransparentIdentifierGJ<object, object>.Inner) };

        //    // TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        //    // need to manipulate the state, not simply copy it over to the grouping
        //    // prolly generate completely new tree - mappings for the grouping should have the Inner added to them,
        //    // basically those should be 2 different mappings

        //    //fix that stuff!!!

        //    var newGrouping = new NavigationExpansionExpression(newGroupingParameter, innerState, groupingParameter.Type);

        //    //var newGrouping = new CustomRootExpression2(
        //    //        new NavigationExpansionExpression(newGroupingParameter, innerState, groupingParameter.Type),
        //    //        groupingMapping,
        //    //        groupingParameter.Type);

        //    var remappedResultSelectorBody = new ExpressionReplacingVisitor(
        //        groupingParameter,
        //        new CustomRootExpression2(newGrouping, groupingMapping, groupingParameter.Type)).Visit(remappedResultSelector.Body);
        //    //remappedResultSelector = Expression.Lambda(remappedResultSelectorBody, outerState.CurrentParameter, newGroupingParameter);

        //    foreach (var outerCustomRootMapping in outerState.CustomRootMappings)
        //    {
        //        outerCustomRootMapping.Insert(0, nameof(TransparentIdentifierGJ<object, object>.Outer));
        //    }

        //    foreach (var outerSourceMapping in outerState.SourceMappings)
        //    {
        //        foreach (var navigationTreeNode in outerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
        //        {
        //            navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifierGJ<object, object>.Outer));
        //            foreach (var fromMapping in navigationTreeNode.FromMappings)
        //            {
        //                fromMapping.Insert(0, nameof(TransparentIdentifierGJ<object, object>.Outer));
        //            }
        //        }
        //    }

        //    var nestedExpansionMappings = outerState.NestedExpansionMappings.ToList();
        //    nestedExpansionMappings.Add(new NestedExpansionMapping(groupingMapping, newGrouping));


        //    //foreach (var innerCustomRootMapping in innerState.CustomRootMappings)
        //    //{
        //    //    innerCustomRootMapping.Insert(0, nameof(TransparentIdentifierGJ<object, object>.InnerGJ));
        //    //}


        //    // TODO: hack !!!!!!!!!!!!!!!!!!!!!!!!!!!!!! <- we prolly need this but commenting for now to see if maybe it works like this by chance


        //    //foreach (var innerSourceMapping in innerState.SourceMappings)
        //    //{
        //    //    foreach (var navigationTreeNode in innerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
        //    //    {
        //    //        navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
        //    //        foreach (var fromMapping in navigationTreeNode.FromMappings)
        //    //        {
        //    //            fromMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
        //    //        }
        //    //    }
        //    //}

        //    var resultType = typeof(TransparentIdentifierGJ<,>).MakeGenericType(outerState.CurrentParameter.Type, newGroupingParameter.Type);
        //    var transparentIdentifierCtorInfo = resultType.GetTypeInfo().GetConstructors().Single();
        //    var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");

        //    var newPendingSelectorBody = new ExpressionReplacingVisitor(outerState.CurrentParameter, transparentIdentifierParameter).Visit(remappedResultSelectorBody);
        //    newPendingSelectorBody = new ExpressionReplacingVisitor(newGroupingParameter, transparentIdentifierParameter).Visit(newPendingSelectorBody);

        //    var newState = new NavigationExpansionExpressionState(
        //        transparentIdentifierParameter,
        //        outerState.SourceMappings.ToList(),
        //        Expression.Lambda(newPendingSelectorBody, transparentIdentifierParameter),
        //        applyPendingSelector: true,
        //        outerState.PendingTerminatingOperator, // TODO: incorrect?
        //        outerState.CustomRootMappings/*,
        //        nestedExpansionMappings*/);

        //    var lambda = Expression.Lambda(
        //        Expression.New(transparentIdentifierCtorInfo, outerState.CurrentParameter, newGroupingParameter),
        //        outerState.CurrentParameter,
        //        newGroupingParameter);

        //    var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
        //        outerResult.state.CurrentParameter.Type,
        //        innerResult.state.CurrentParameter.Type,
        //        outerResult.lambdaBody.Type,
        //        lambda.Body.Type);

        //    var rewritten = Expression.Call(
        //        newMethodInfo,
        //        outerResult.source,
        //        innerResult.source,
        //        Expression.Lambda(newOuterKeySelectorBody, outerResult.state.CurrentParameter),
        //        Expression.Lambda(newInnerKeySelectorBody, innerResult.state.CurrentParameter),
        //        lambda);

        //    // TODO: expand navigations in the result selector of the GroupJoin!!!

        //    return new NavigationExpansionExpression(
        //        rewritten,
        //        newState,
        //        methodCallExpression.Type);
        //}

        private bool IsQueryable(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                return true;
            }

            return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
        }

        private Expression ProcessTerminatingOperation(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableCountPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAnyPredicateMethodInfo))
            {
                return Visit(SimplifyPredicateMethod(methodCallExpression, queryable: true));
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(EnumerableCountPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableAnyPredicateMethodInfo))
            {
                return Visit(SimplifyPredicateMethod(methodCallExpression, queryable: false));
            }

            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState(Expression.Parameter(source.Type.GetSequenceType()));
            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;

                if (state.ApplyPendingSelector)
                {
                    var unbinder = new NavigationPropertyUnbindingVisitor(state.CurrentParameter);
                    var newSelectorBody = unbinder.Visit(state.PendingSelector.Body);

                    // TODO: test cases with casts, e.g. orders.Select(o => new { foo = (VipCustomer)o.Customer }).Distinct()
                    var entityTypeOverride = methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)
                        ? _model.FindEntityType(methodCallExpression.Method.GetGenericArguments()[0])
                        : null;

                    var pssmg = new PendingSelectorSourceMappingGenerator(state.PendingSelector.Parameters[0], entityTypeOverride);
                    pssmg.Visit(state.PendingSelector);

                    var selectorMethodInfo = IsQueryable(navigationExpansionExpression.Operand.Type)
                        ? QueryableSelectMethodInfo
                        : EnumerableSelectMethodInfo;

                    selectorMethodInfo = selectorMethodInfo.MakeGenericMethod(
                        state.CurrentParameter.Type,
                        newSelectorBody.Type);

                    var result = Expression.Call(
                        selectorMethodInfo,
                        navigationExpansionExpression.Operand,
                        Expression.Lambda(newSelectorBody, state.CurrentParameter));

                    var newPendingSelectorParameter = Expression.Parameter(newSelectorBody.Type);
                    var customRootMapping = new List<string>();

                    // if the top level was navigation binding, then we are guaranteed to have exactly one source mapping in for the new pending selector
                    var newPendingSelectorBody = state.PendingSelector.Body is NavigationBindingExpression binding
                        ? (Expression)new NavigationBindingExpression(
                            newPendingSelectorParameter,
                            pssmg.BindingToSourceMapping[binding].NavigationTree,
                            pssmg.BindingToSourceMapping[binding].RootEntityType,
                            pssmg.BindingToSourceMapping[binding],
                            newPendingSelectorParameter.Type)
                        : new CustomRootExpression(newPendingSelectorParameter, customRootMapping, newPendingSelectorParameter.Type);

                    // TODO: only apply custom root mapping for parameters that are not root!
                    state = new NavigationExpansionExpressionState(
                        newPendingSelectorParameter,
                        pssmg.SourceMappings,
                        Expression.Lambda(newPendingSelectorBody, newPendingSelectorParameter),
                        applyPendingSelector: false,
                        pendingTerminatingOperator: null,
                        new List<List<string>> { customRootMapping }/*,
                        new List<NestedExpansionMapping>()*/);

                    if (/*methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOfType)*/

                        // TODO: fix this, use method infos instead, if this is the right place to handle Enumerables
                        methodCallExpression.Method.Name == "Distinct"
                        || methodCallExpression.Method.Name == "First"
                        || methodCallExpression.Method.Name == "FirstOrDefault"
                        || methodCallExpression.Method.Name == "Single"
                        || methodCallExpression.Method.Name == "SingleOrDefault"
                        || methodCallExpression.Method.Name == "Any"
                        || methodCallExpression.Method.Name == "OfType"

                        || methodCallExpression.Method.Name == "AsTracking"
                        || methodCallExpression.Method.Name == "AsNoTracking")
                    {
                        var newMethod = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                            result.Type.GetGenericArguments()[0]);

                        source = Expression.Call(newMethod, new[] { result });
                    }
                    else if (/*methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSkipMethodInfo)*/
                        methodCallExpression.Method.Name == "Take"
                        || methodCallExpression.Method.Name == "Skip"
                        /*|| methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains)*/)
                    {
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
                    var newArguments = methodCallExpression.Arguments.ToList();
                    newArguments[0] = source;
                    source = methodCallExpression.Update(methodCallExpression.Object, newArguments);

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

        private MethodCallExpression SimplifyPredicateMethod(MethodCallExpression methodCallExpression, bool queryable)
        {
            var whereMethodInfo = queryable
                ? QueryableWhereMethodInfo
                : EnumerableWhereMethodInfo;

            var typeArgument = methodCallExpression.Arguments[0].Type.GetSequenceType();
            whereMethodInfo = whereMethodInfo.MakeGenericMethod(typeArgument);
            var whereMethodCall = Expression.Call(whereMethodInfo, methodCallExpression.Arguments[0], methodCallExpression.Arguments[1]);

            var newMethodInfo = GetNewMethodInfo(methodCallExpression.Method.Name, queryable);
            newMethodInfo = newMethodInfo.MakeGenericMethod(typeArgument);

            return Expression.Call(newMethodInfo, whereMethodCall);
        }

        private MethodInfo GetNewMethodInfo(string name, bool queryable)
        {
            if (queryable)
            {
                switch (name)
                {
                    case nameof(Queryable.Count):
                        return QueryableCountMethodInfo;

                    case nameof(Queryable.First):
                        return QueryableFirstMethodInfo;

                    case nameof(Queryable.FirstOrDefault):
                        return QueryableFirstOrDefaultMethodInfo;

                    case nameof(Queryable.Single):
                        return QueryableSingleMethodInfo;

                    case nameof(Queryable.SingleOrDefault):
                        return QueryableSingleOrDefaultMethodInfo;

                    case nameof(Queryable.Any):
                        return QueryableAnyMethodInfo;
                }
            }
            else
            {
                switch (name)
                {
                    case nameof(Enumerable.Count):
                        return EnumerableCountMethodInfo;

                    case nameof(Enumerable.First):
                        return EnumerableFirstMethodInfo;

                    case nameof(Enumerable.FirstOrDefault):
                        return EnumerableFirstOrDefaultMethodInfo;

                    case nameof(Enumerable.Single):
                        return EnumerableSingleMethodInfo;

                    case nameof(Enumerable.SingleOrDefault):
                        return EnumerableSingleOrDefaultMethodInfo;

                    case nameof(Enumerable.Any):
                        return EnumerableAnyMethodInfo;
                }
            }

            throw new InvalidOperationException("Invalid method name: " + name);
        }

        private Expression ProcessCardinalityReducingOperation(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSinglePredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultPredicateMethodInfo))
            {
                return Visit(SimplifyPredicateMethod(methodCallExpression, queryable: true));
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(EnumerableFirstPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableFirstOrDefaultPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableSinglePredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(EnumerableSingleOrDefaultPredicateMethodInfo))
            {
                return Visit(SimplifyPredicateMethod(methodCallExpression, queryable: false));
            }

            var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            var state = new NavigationExpansionExpressionState(Expression.Parameter(source.Type.GetSequenceType()));
            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;
            }

            state.PendingTerminatingOperator = methodCallExpression.Method.GetGenericMethodDefinition();

            return new NavigationExpansionExpression(source, state, methodCallExpression.Type);
        }

        private Expression ProcessContains(MethodCallExpression methodCallExpression)
        {
            return base.VisitMethodCall(methodCallExpression);
            //var source = VisitSourceExpression(methodCallExpression.Arguments[0]);
            //var state = new NavigationExpansionExpressionState(Expression.Parameter(source.Type.GetSequenceType()));
            //if (source is NavigationExpansionExpression navigationExpansionExpression)
            //{
            //    source = navigationExpansionExpression.Operand;
            //    state = AdjustState(state, navigationExpansionExpression);
            //}
            //else
            //{
            //    return methodCallExpression.Update(methodCallExpression.Object, )
            //    throw new InvalidOperationException("not handling nav expansion correctly");
            //}

            //var operand = Visit(methodCallExpression.Arguments[0]);

            //return methodCallExpression;
        }

        protected override Expression VisitConstant(ConstantExpression constantExpression)
        {
            if (constantExpression.Value != null
                && constantExpression.Value.GetType().IsGenericType
                && constantExpression.Value.GetType().GetGenericTypeDefinition() == typeof(EntityQueryable<>))
            {
                var elementType = constantExpression.Value.GetType().GetSequenceType();
                var entityType = _model.FindEntityType(elementType);

                var sourceMapping = new SourceMapping
                {
                    RootEntityType = entityType,
                };

                var navigationTreeRoot = NavigationTreeNode.CreateRoot(sourceMapping, fromMapping: new List<string>(), optional: false);
                sourceMapping.NavigationTree = navigationTreeRoot;

                var pendingSelectorParameter = Expression.Parameter(entityType.ClrType);
                var pendingSelector = Expression.Lambda(
                    new NavigationBindingExpression(
                        pendingSelectorParameter,
                        navigationTreeRoot,
                        entityType,
                        sourceMapping,
                        pendingSelectorParameter.Type),
                    pendingSelectorParameter);

                var result = new NavigationExpansionExpression(
                    constantExpression,
                    new NavigationExpansionExpressionState(
                        pendingSelectorParameter,
                        new List<SourceMapping> { sourceMapping },
                        pendingSelector,
                        applyPendingSelector: false,
                        pendingTerminatingOperator: null,
                        new List<List<string>>()/*,
                        new List<NestedExpansionMapping>()*/),
                    constantExpression.Type);

                return result;
            }

            return base.VisitConstant(constantExpression);
        }

        private (Expression source, Expression lambdaBody, NavigationExpansionExpressionState state) FindAndApplyNavigations(
            Expression source,
            LambdaExpression lambda,
            NavigationExpansionExpressionState state)
        {
            if (state.PendingSelector == null)
            {
                return (source, lambda.Body, state);
            }

            var remappedLambdaBody = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, lambda).Body;

            var binder = new NavigationPropertyBindingVisitor(
                state.PendingSelector.Parameters[0],
                state.SourceMappings);

            var boundLambdaBody = binder.Visit(remappedLambdaBody);
            boundLambdaBody = new NavigationComparisonOptimizingVisitor().Visit(boundLambdaBody);
            boundLambdaBody = new CollectionNavigationRewritingVisitor(state.CurrentParameter).Visit(boundLambdaBody);
            boundLambdaBody = Visit(boundLambdaBody);

            var result = (source, parameter: state.CurrentParameter);
            var applyPendingSelector = state.ApplyPendingSelector;

            foreach (var sourceMapping in state.SourceMappings)
            {
                if (sourceMapping.NavigationTree.Flatten().Any(n => !n.Expanded))
                {
                    foreach (var navigationTree in sourceMapping.NavigationTree.Children)
                    {
                        if (navigationTree.Navigation.IsCollection())
                        {
                            throw new InvalidOperationException("Collections should not be part of the navigation tree: " + navigationTree.Navigation);
                        }

                        result = AddNavigationJoin(
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

            var newState = new NavigationExpansionExpressionState(
                result.parameter,
                state.SourceMappings,
                pendingSelector,
                applyPendingSelector,
                state.PendingTerminatingOperator, // TODO: should we keep it?
                state.CustomRootMappings/*,
                state.NestedExpansionMappings*/);

            // TODO: improve this (maybe a helper method?)
            if (source.Type.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)
                && result.source.Type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                var toOrderedMethod = typeof(NavigationExpansionExpression).GetMethod(nameof(NavigationExpansionExpression.ToOrdered)).MakeGenericMethod(result.source.Type.GetSequenceType());
                var toOrderedCall = Expression.Call(toOrderedMethod, result.source);

                return (source: toOrderedCall, lambdaBody: boundLambdaBody, state: newState);
            }

            return (result.source, lambdaBody: boundLambdaBody, state: newState);
        }

        private (Expression source, ParameterExpression parameter) AddNavigationJoin(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            SourceMapping sourceMapping,
            NavigationTreeNode navigationTree,
            NavigationExpansionExpressionState state,
            List<INavigation> navigationPath)
        {
            if (!navigationTree.Expanded)
            {
                // TODO: hack - if we wrapped collection around MaterializeCollectionNavigation during collection rewrite, unwrap that call when applying navigations on top
                if (sourceExpression is MethodCallExpression sourceMethodCall
                    && sourceMethodCall.Method.Name == "MaterializeCollectionNavigation")
                {
                    sourceExpression = sourceMethodCall.Arguments[1];
                }

                var navigation = navigationTree.Navigation;
                var sourceType = sourceExpression.Type.GetSequenceType();
                var navigationTargetEntityType = navigation.GetTargetType();

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, null, navigationTree.Parent.ToMapping);

                var outerKeySelectorBody = NavigationExpansionHelpers.CreateKeyAccessExpression(
                    transparentIdentifierAccessorExpression,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.Properties
                        : navigation.ForeignKey.PrincipalKey.Properties,
                    addNullCheck: navigationTree.Parent != null && navigationTree.Parent.Optional);

                var innerKeySelectorParameterType = navigationTargetEntityType.ClrType;
                var innerKeySelectorParameter = Expression.Parameter(
                    innerKeySelectorParameterType,
                    parameterExpression.Name + "." + navigationTree.Navigation.Name);

                var innerKeySelectorBody = NavigationExpansionHelpers.CreateKeyAccessExpression(
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

                    // TODO: massive hack!!!!
                    if (sourceExpression.Type.ToString().StartsWith("System.Collections.Generic.IEnumerable`1[")
                        || sourceExpression.Type.ToString().StartsWith("System.Linq.IOrderedEnumerable`1["))
                    {
                        groupJoinMethodInfo = EnumerableGroupJoinMethodInfo.MakeGenericMethod(
                        sourceType,
                        navigationTargetEntityType.ClrType,
                        outerKeySelector.Body.Type,
                        groupJoinResultType);
                    }

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

                    // TODO: massive hack!!!!
                    if (groupJoinMethodCall.Type.ToString().StartsWith("System.Collections.Generic.IEnumerable`1[")
                        || groupJoinMethodCall.Type.ToString().StartsWith("System.Linq.IOrderedEnumerable`1["))
                    {
                        selectManyMethodInfo = EnumerableSelectManyWithResultOperatorMethodInfo.MakeGenericMethod(
                            groupJoinResultType,
                            navigationTargetEntityType.ClrType,
                            selectManyResultType);
                    }

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

                    // TODO: massive hack!!!!
                    if (sourceExpression.Type.ToString().StartsWith("System.Collections.Generic.IEnumerable`1[")
                        || sourceExpression.Type.ToString().StartsWith("System.Linq.IOrderedEnumerable`1["))
                    {
                        joinMethodInfo = EnumerableJoinMethodInfo.MakeGenericMethod(
                        sourceType,
                        navigationTargetEntityType.ClrType,
                        outerKeySelector.Body.Type,
                        resultType);
                    }

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
                navigationTree.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Inner));
                foreach (var mapping in state.SourceMappings)
                {
                    foreach (var navigationTreeNode in mapping.NavigationTree.Flatten().Where(n => n.Expanded && n != navigationTree))
                    {
                        navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        if (navigationTree.Optional)
                        {
                            navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        }
                    }
                }

                foreach (var customRootMapping in state.CustomRootMappings)
                {
                    customRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    if (navigationTree.Optional)
                    {
                        customRootMapping.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    }
                }

                navigationTree.Expanded = true;
                navigationPath.Add(navigation);
            }
            else
            {
                navigationPath.Add(navigationTree.Navigation);
            }

            var result = (source: sourceExpression, parameter: parameterExpression);
            foreach (var child in navigationTree.Children)
            {
                result = AddNavigationJoin(
                    result.source,
                    result.parameter,
                    sourceMapping,
                    child,
                    state,
                    navigationPath.ToList());
            }

            return result;
        }

        private void RemapNavigationChain(NavigationTreeNode navigationTreeNode, bool optional)
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

            var outerBinder = new NavigationPropertyBindingVisitor(
                outerState.CurrentParameter,
                outerState.SourceMappings);

            var innerBinder = new NavigationPropertyBindingVisitor(
                innerState.CurrentParameter,
                innerState.SourceMappings);

            var boundResultSelectorBody = outerBinder.Visit(remappedResultSelector.Body);
            boundResultSelectorBody = innerBinder.Visit(boundResultSelectorBody);

            foreach (var outerCustomRootMapping in outerState.CustomRootMappings)
            {
                outerCustomRootMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Outer));
            }

            //foreach (var outerNestedExpansionMapping in outerState.NestedExpansionMappings)
            //{
            //    outerNestedExpansionMapping.Path.Insert(0, nameof(TransparentIdentifier2<object, object>.Outer));
            //}

            foreach (var outerSourceMapping in outerState.SourceMappings)
            {
                foreach (var navigationTreeNode in outerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
                {
                    navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Outer));
                    foreach (var fromMapping in navigationTreeNode.FromMappings)
                    {
                        fromMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Outer));
                    }
                }
            }

            foreach (var innerCustomRootMapping in innerState.CustomRootMappings)
            {
                innerCustomRootMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Inner));
            }

            //foreach (var innerNestedExpansionMapping in innerState.NestedExpansionMappings)
            //{
            //    innerNestedExpansionMapping.Path.Insert(0, nameof(TransparentIdentifier2<object, object>.Inner));
            //}

            foreach (var innerSourceMapping in innerState.SourceMappings)
            {
                foreach (var navigationTreeNode in innerSourceMapping.NavigationTree.Flatten().Where(n => n.Expanded))
                {
                    navigationTreeNode.ToMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Inner));
                    foreach (var fromMapping in navigationTreeNode.FromMappings)
                    {
                        fromMapping.Insert(0, nameof(TransparentIdentifier2<object, object>.Inner));
                    }
                }
            }

            var resultType = typeof(TransparentIdentifier2<,>).MakeGenericType(outerState.CurrentParameter.Type, innerState.CurrentParameter.Type);
            var transparentIdentifierCtorInfo = resultType.GetTypeInfo().GetConstructors().Single();
            var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");

            var newPendingSelectorBody = new ExpressionReplacingVisitor(outerState.CurrentParameter, transparentIdentifierParameter).Visit(boundResultSelectorBody);
            newPendingSelectorBody = new ExpressionReplacingVisitor(innerState.CurrentParameter, transparentIdentifierParameter).Visit(newPendingSelectorBody);

            var newState = new NavigationExpansionExpressionState(
                transparentIdentifierParameter,
                outerState.SourceMappings.Concat(innerState.SourceMappings).ToList(),
                Expression.Lambda(newPendingSelectorBody, transparentIdentifierParameter),
                applyPendingSelector: true,
                pendingTerminatingOperator: null, // TODO: incorrect?
                outerState.CustomRootMappings.Concat(innerState.CustomRootMappings).ToList()/*,
                outerState.NestedExpansionMappings.Concat(innerState.NestedExpansionMappings).ToList()*/);

            var lambda = Expression.Lambda(
                Expression.New(transparentIdentifierCtorInfo, outerState.CurrentParameter, innerState.CurrentParameter),
                outerState.CurrentParameter,
                innerState.CurrentParameter);

            return (lambda, state: newState);
        }

        private (LambdaExpression lambda, NavigationExpansionExpressionState state) RemapGroupJoinResultSelector(
            LambdaExpression resultSelector,
            NavigationExpansionExpressionState outerState)
        {

            return (null, null);
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
                    result = Expression.PropertyOrField(result, accessorPathElement);
                }
            }

            return result;
        }

        private class PendingSelectorSourceMappingGenerator : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();
            private IEntityType _entityTypeOverride;

            public List<SourceMapping> SourceMappings = new List<SourceMapping>();

            public Dictionary<NavigationBindingExpression, SourceMapping> BindingToSourceMapping
                = new Dictionary<NavigationBindingExpression, SourceMapping>();

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
                if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
                {
                    if (navigationBindingExpression.RootParameter == _rootParameter)
                    {
                        var sourceMapping = new SourceMapping
                        {
                            RootEntityType = _entityTypeOverride ?? navigationBindingExpression.EntityType,
                        };

                        var navigationTreeRoot = NavigationTreeNode.CreateRoot(sourceMapping, _currentPath.ToList(), navigationBindingExpression.NavigationTreeNode.Optional);
                        sourceMapping.NavigationTree = navigationTreeRoot;

                        SourceMappings.Add(sourceMapping);
                        BindingToSourceMapping[navigationBindingExpression] = sourceMapping;
                    }

                    return extensionExpression;
                }

                // TODO: is this correct or some processing is needed here?
                if (extensionExpression is CustomRootExpression customRootExpression)
                {
                    return customRootExpression;
                }

                return base.VisitExtension(extensionExpression);
            }
        }

        private class ExpressionReplacingVisitor : NavigationExpansionVisitorBase
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
