// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Extensions.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal
{
    public static class ExpressionExtensions
    {
        public static LambdaExpression UnwrapQuote(this Expression expression)
            => expression is UnaryExpression unary && expression.NodeType == ExpressionType.Quote
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expression;

        public static bool IsIncludeMethod(this MethodCallExpression methodCallExpression)
            => methodCallExpression.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && methodCallExpression.Method.Name == nameof(EntityFrameworkQueryableExtensions.Include);

        public static LambdaExpression CombineAndRemapLambdas(
            LambdaExpression first,
            LambdaExpression second)
            => CombineAndRemapLambdas(first, second, second.Parameters[0]);

        public static LambdaExpression CombineAndRemapLambdas(
            LambdaExpression first,
            LambdaExpression second,
            ParameterExpression secondLambdaParameterToReplace)
        {
            if (first == null)
            {
                return second;
            }

            if (second == null)
            {
                return first;
            }

            var lcev = new LambdaCombiningExpressionVisitor(first, first.Parameters[0], secondLambdaParameterToReplace);

            return (LambdaExpression)lcev.Visit(second);
        }
    }

    // TODO: temporary hack
    public static class ParameterNamingExtensions
    {
        public static string GenerateParameterName(this Type type)
        {
            var sb = new StringBuilder();
            var removeLowerCase = sb.Append(type.Name.Where(c => char.IsUpper(c)).ToArray()).ToString();

            if (removeLowerCase.Length > 0)
            {
                return removeLowerCase.ToLower();
            }
            else
            {
                return type.Name.ToLower().Substring(0, 1);
            }
        }
    }

    public readonly struct TransparentIdentifier<TOuter, TInner>
    {
        [UsedImplicitly]
        public TransparentIdentifier(TOuter outer, TInner inner)
        {
            Outer = outer;
            Inner = inner;
        }

        [UsedImplicitly]
        public readonly TOuter Outer;

        [UsedImplicitly]
        public readonly TInner Inner;
    }


    public class NavigationPropertyBindingExpressionVisitor2 : ExpressionVisitor
    {
        private ParameterExpression _rootParameter;
        private List<SourceMapping> _sourceMappings;

        public NavigationPropertyBindingExpressionVisitor2(
            ParameterExpression rootParameter,
            List<SourceMapping> sourceMappings)
        {
            _rootParameter = rootParameter;
            _sourceMappings = sourceMappings;
        }

        private (ParameterExpression rootParameter, List<INavigation> navigations) TryFindMatchingTransparentIdentifierMapping(
            Expression expression,
            List<string> initialPath,
            List<(List<string> path, List<INavigation> navigations)> transparentIdentifierMappingCandidates)
        {
            if (expression is ParameterExpression parameterExpression
                && (parameterExpression == _rootParameter || _rootParameter == null)
                && initialPath.Count == 0)
            {
                var matchingCandidate = transparentIdentifierMappingCandidates.Where(m => m.path.Count == 0).SingleOrDefault();

                return matchingCandidate.navigations != null
                    ? (rootParameter: parameterExpression, matchingCandidate.navigations)
                    : (null, null);
            }

            if (expression is MemberExpression memberExpression)
            {
                var matchingCandidates = transparentIdentifierMappingCandidates.Where(m => m.path.Count > 0 && m.path.Last() == memberExpression.Member.Name);
                var newCandidates = matchingCandidates.Select(mc => (path: mc.path.Take(mc.path.Count - 1).ToList(), mc.navigations.ToList())).ToList();
                if (newCandidates.Any())
                {
                    var result = TryFindMatchingTransparentIdentifierMapping(memberExpression.Expression, initialPath, newCandidates);
                    if (result.rootParameter != null)
                    {
                        return result;
                    }
                }

                if (initialPath.Count > 0 && memberExpression.Member.Name == initialPath.Last())
                {
                    var emptyCandidates = transparentIdentifierMappingCandidates.Where(m => m.path.Count == 0).ToList();
                    if (emptyCandidates.Count > 0)
                    {
                        return TryFindMatchingTransparentIdentifierMapping(memberExpression.Expression, initialPath.Take(initialPath.Count - 1).ToList(), emptyCandidates);
                    }
                }
            }

            return (null, null);
        }

        protected override Expression VisitExtension(Expression extensionExpression)
            => extensionExpression;

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var newExpression = Visit(memberExpression.Expression);

            if (newExpression is NavigationBindingExpression2 navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _rootParameter)
                {
                    var navigation = navigationBindingExpression.EntityType.FindNavigation(memberExpression.Member.Name);
                    if (navigation != null)
                    {
                        var navigations = navigationBindingExpression.Navigations.ToList();
                        navigations.Add(navigation);

                        return new NavigationBindingExpression2(
                            memberExpression,
                            navigationBindingExpression.RootParameter,
                            navigations,
                            navigation.GetTargetType(),
                            navigationBindingExpression.SourceMapping);
                    }
                }
            }
            else
            {
                foreach (var sourceMapping in _sourceMappings)
                {
                    var match = TryFindMatchingTransparentIdentifierMapping(memberExpression, sourceMapping.InitialPath, sourceMapping.TransparentIdentifierMapping);
                    if (match.rootParameter != null)
                    {
                        return new NavigationBindingExpression2(
                            memberExpression,
                            match.rootParameter,
                            match.navigations,
                            match.navigations.Count > 0 ? match.navigations.Last().GetTargetType() : sourceMapping.RootEntityType,
                            sourceMapping);
                    }
                }
            }

            var newMemberExpression = memberExpression.Update(newExpression);

            return newMemberExpression;
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newBody = Visit(lambdaExpression.Body);

            return newBody != lambdaExpression.Body
                ? Expression.Lambda(newBody, lambdaExpression.Parameters)
                : lambdaExpression;
        }


        protected override Expression VisitParameter(ParameterExpression parameterExpression)
        {
            if (parameterExpression == _rootParameter
                || _rootParameter == null)
            {
                var sourceMapping = _sourceMappings.Where(sm => sm.RootEntityType.ClrType == parameterExpression.Type && sm.InitialPath.Count == 0).SingleOrDefault();
                if (sourceMapping != null)
                {
                    return new NavigationBindingExpression2(
                        parameterExpression,
                        parameterExpression,
                        new List<INavigation>(),
                        sourceMapping.RootEntityType,
                        sourceMapping);
                }
            }

            return parameterExpression;//  base.VisitParameter(parameterExpression);
        }
    }

    public class NavigationBindingExpression2 : Expression
    {
        public Expression Operand { get; }
        public ParameterExpression RootParameter { get; }
        public IEntityType EntityType { get; }
        public IReadOnlyList<INavigation> Navigations { get; }
        public SourceMapping SourceMapping { get; }

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override bool CanReduce => true;
        public override Type Type => Operand.Type;

        public override Expression Reduce()
            => Operand;

        public NavigationBindingExpression2(Expression operand, ParameterExpression rootParameter, List<INavigation> navigations, IEntityType entityType, SourceMapping sourceMapping)
        {
            Operand = operand;
            RootParameter = rootParameter;
            Navigations = navigations.AsReadOnly();
            EntityType = entityType;
            SourceMapping = sourceMapping;
        }
    }

    public class NavigationExpander
    {
        private IModel _model;

        public NavigationExpander([NotNull] IModel model)
        {
            Check.NotNull(model, nameof(model));

            _model = model;
        }

        public virtual Expression ExpandNavigations(Expression expression)
        {
            var navigationExpandingExpressionVisitor = new NavigationExpandingExpressionVisitor(_model);
            var newExpression = navigationExpandingExpressionVisitor.Visit(expression);
            newExpression = new ReducingVisitor().Visit(newExpression);

            return newExpression;
        }

        private class ReducingVisitor : ExpressionVisitor
        {
        }
    }

    public class LambdaCombiningExpressionVisitor : ExpressionVisitor
    {
        private LambdaExpression _newSelector;
        private ParameterExpression _newParameter;
        private ParameterExpression _previousParameter;

        public LambdaCombiningExpressionVisitor(
            LambdaExpression newSelector,
            ParameterExpression newParameter,
            ParameterExpression previousParameter)
        {
            _newSelector = newSelector;
            _newParameter = newParameter;
            _previousParameter = previousParameter;
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            // TODO: combine this with navigation replacing expression visitor? logic is the same
            var newParameters = new List<ParameterExpression>();
            var parameterChanged = false;

            foreach (var parameter in lambdaExpression.Parameters)
            {
                if (parameter == _previousParameter)
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
        {
            if (parameterExpression == _previousParameter)
            {
                var prev = new ParameterReplacingExpressionVisitor(parameterToReplace: _previousParameter, replaceWith: _newSelector.Body);
                var result = prev.Visit(parameterExpression);

                return result;
            }

            return base.VisitParameter(parameterExpression);
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var newSource = Visit(memberExpression.Expression);
            if (newSource is NewExpression newExpression)
            {
                var matchingMemberIndex = newExpression.Members.Select((m, i) => new { index = i, match = m == memberExpression.Member }).Where(r => r.match).SingleOrDefault()?.index;
                if (matchingMemberIndex.HasValue)
                {
                    return newExpression.Arguments[matchingMemberIndex.Value];
                }
            }

            return newSource != memberExpression.Expression
                ? memberExpression.Update(newSource)
                : memberExpression;
        }

        private class ParameterReplacingExpressionVisitor : ExpressionVisitor
        {
            private ParameterExpression _parameterToReplace;
            private Expression _replaceWith;

            public ParameterReplacingExpressionVisitor(ParameterExpression parameterToReplace, Expression replaceWith)
            {
                _parameterToReplace = parameterToReplace;
                _replaceWith = replaceWith;
            }

            protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
            {
                var newBody = Visit(lambdaExpression.Body);

                return newBody != lambdaExpression.Body
                    ? Expression.Lambda(newBody, lambdaExpression.Parameters)
                    : lambdaExpression;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
                => parameterExpression == _parameterToReplace
                ? _replaceWith
                : parameterExpression;
        }
    }

    public class NavigationExpandingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        private IModel _model;

        public NavigationExpandingExpressionVisitor(IModel model)
        {
            _model = model;
        }

        private class EntityTypeAccessorMappingGenerator2 : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<string> _currentPath = new List<string>();

            public EntityTypeAccessorMappingGenerator2(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
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
                        var sourceMapping = navigationBindingExpression.SourceMapping;
                        sourceMapping.InitialPath = _currentPath.ToList(); 
                        sourceMapping.RootEntityType = navigationBindingExpression.EntityType;
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

        //private class EntityTypeAccessorMappingGenerator : ExpressionVisitor
        //{
        //    private ParameterExpression _rootParameter;
        //    private List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)> _navigationExpansionMapping;
        //    private List<string> _currentPath;
        //    public List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)> NewNavigationExpansionMapping { get; }

        //    public EntityTypeAccessorMappingGenerator(
        //        ParameterExpression rootParameter,
        //        List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)> navigationExpansionMapping)
        //    {
        //        _rootParameter = rootParameter;
        //        _navigationExpansionMapping = navigationExpansionMapping;
        //        _currentPath = new List<string>();
        //        NewNavigationExpansionMapping = new List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)>();
        //    }

        //    // prune these nodes, we only want to look for entities accessible in the result
        //    protected override Expression VisitMember(MemberExpression memberExpression)
        //        => memberExpression;

        //    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        //        => methodCallExpression;

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
        //                NewNavigationExpansionMapping.Add((path: _currentPath.ToList(), initialPath: _currentPath.ToList(), rootEntityType: navigationBindingExpression.EntityType, navigations: new List<INavigation>()/* navigationBindingExpression.Navigations.ToList()*/));
        //            }

        //            return navigationBindingExpression;
        //        }

        //        return base.VisitExtension(extensionExpression);
        //    }
        //}

        protected override Expression VisitExtension(Expression extensionExpression)
            => extensionExpression;

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableWhereMethodInfo))
            {
                var result = ProcessWhere(methodCallExpression);

                return result;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectMethodInfo))
            {
                var result = ProcessSelect(methodCallExpression);

                return result;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableOrderByMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableOrderByDescendingMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableThenByMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableThenByDescendingMethodInfo))
            {
                var result = ProcessOrderBy(methodCallExpression);

                return result;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
            {
                var result = ProcessSelectManyWithResultOperator(methodCallExpression);

                return result;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableJoinMethodInfo))
            {
                var result = ProcessJoin(methodCallExpression);

                return result;
            }

            //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableGroupJoinMethodInfo))
            //{
            //    var result = ProcessGroupJoin(methodCallExpression);

            //    return result;
            //}

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains))
            {
                var result = ProcessTerminatingOperation(methodCallExpression);

                return result;
            }

            return base.VisitMethodCall(methodCallExpression);
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

                // TODO: fix this!
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;
                state.PendingSelector = state.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            var combinedPredicate = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, predicate);
            combinedPredicate = (LambdaExpression)Visit(combinedPredicate);

            var result = FindAndApplyNavigations(source, combinedPredicate, state);

            var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.state.CurrentParameter.Type);
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
            var state = new NavigationExpansionExpressionState
            {
                CurrentParameter = selector.Parameters[0]
            };

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;

                // TODO: fix this!
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;
                state.PendingSelector = state.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            var combinedSelector = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, selector.UnwrapQuote());
            combinedSelector = (LambdaExpression)Visit(combinedSelector);

            var result = FindAndApplyNavigations(source, combinedSelector, state);
            result.state.PendingSelector = (LambdaExpression)result.lambda;

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

                // TODO: fix this!
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;
                state.PendingSelector = state.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            var combinedKeySelector = ExpressionExtensions.CombineAndRemapLambdas(state.PendingSelector, keySelector.UnwrapQuote());
            combinedKeySelector = (LambdaExpression)Visit(combinedKeySelector);

            var result = FindAndApplyNavigations(source, combinedKeySelector, state);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.state.CurrentParameter.Type, result.lambda.UnwrapQuote().Body.Type);
            var rewritten = Expression.Call(newMethodInfo, result.source, result.lambda);

            return new NavigationExpansionExpression(
                rewritten,
                result.state,
                methodCallExpression.Type);
        }

        private Expression ProcessSelectManyWithResultOperator(MethodCallExpression methodCallExpression)
        {
            //var outerSource = Visit(methodCallExpression.Arguments[0]);
            //var innerSource = Visit(methodCallExpression.Arguments[1]);

            //var resultSelector = methodCallExpression.Arguments[2];
            //var outerCurrentParameter = resultSelector.UnwrapQuote().Parameters[0];
            //var innerCurrentParameter = resultSelector.UnwrapQuote().Parameters[1];
            //var outerFirstSelectorParameter = outerCurrentParameter;
            //var innerFirstSelectorParameter = innerCurrentParameter;
            //var outerOriginalParameter = outerCurrentParameter;
            //var innerOriginalrParameter = innerCurrentParameter;

            //var outerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            //var outerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            //var outerPendingSelector = default(LambdaExpression);
            //var outerFoundNavigations = new List<NavigationTreeNode>();
            //var outerFinalProjectionPath = new List<string>();

            //var innerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            //var innerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            //var innerPendingSelector = default(LambdaExpression);
            //var innerFoundNavigations = new List<NavigationTreeNode>();
            //var innerFinalProjectionPath = new List<string>();

            //// TODO: unwrap lambda body here
            //if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            //{
            //    outerSource = outerNavigationExpansionExpression.Operand;
            //    outerTransparentIdentifierAccessorMapping = outerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
            //    outerEntityTypeAccessorMapping = outerNavigationExpansionExpression.EntityTypeAccessorMapping;
            //    outerPendingSelector = outerNavigationExpansionExpression.PendingSelector;
            //    outerFoundNavigations = outerNavigationExpansionExpression.FoundNavigations;
            //    outerFinalProjectionPath = outerNavigationExpansionExpression.FinalProjectionPath;
            //}

            //if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            //{
            //    innerSource = innerNavigationExpansionExpression.Operand;
            //    innerTransparentIdentifierAccessorMapping = innerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
            //    innerEntityTypeAccessorMapping = innerNavigationExpansionExpression.EntityTypeAccessorMapping;
            //    innerPendingSelector = innerNavigationExpansionExpression.PendingSelector;
            //    innerFoundNavigations = innerNavigationExpansionExpression.FoundNavigations;
            //    innerFinalProjectionPath = innerNavigationExpansionExpression.FinalProjectionPath;
            //}

            //// TODO: finish this!!!

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

                // TODO: fix this!
                var currentParameter = outerState.CurrentParameter;
                outerState = outerNavigationExpansionExpression.State;
                outerState.CurrentParameter = outerState.CurrentParameter ?? currentParameter;
                outerState.PendingSelector = outerState.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            {
                innerSource = innerNavigationExpansionExpression.Operand;

                // TODO: fix this!
                var currentParameter = innerState.CurrentParameter;
                innerState = innerNavigationExpansionExpression.State;
                innerState.CurrentParameter = innerState.CurrentParameter ?? currentParameter;
                innerState.PendingSelector = innerState.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
            }

            var combinedOuterKeySelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, outerKeySelector);
            combinedOuterKeySelector = (LambdaExpression)Visit(combinedOuterKeySelector);

            var combinedInnerKeySelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, innerKeySelector);
            combinedInnerKeySelector = (LambdaExpression)Visit(combinedInnerKeySelector);

            var outerResult = FindAndApplyNavigations(outerSource, combinedOuterKeySelector, outerState);
            var innerResult = FindAndApplyNavigations(innerSource, combinedInnerKeySelector, innerState);

            // remap result selector into transparent identifier
            var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(outerResult.state.CurrentParameter.Type, innerResult.state.CurrentParameter.Type);

            var transparentIdentifierCtorInfo
                = resultType.GetTypeInfo().GetConstructors().Single();

            var newResultSelector = Expression.Lambda(
                Expression.New(transparentIdentifierCtorInfo, outerResult.state.CurrentParameter, innerResult.state.CurrentParameter),
                outerResult.state.CurrentParameter,
                innerResult.state.CurrentParameter);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                outerResult.state.CurrentParameter.Type,
                innerResult.state.CurrentParameter.Type,
                outerResult.lambda.UnwrapQuote().Body.Type,
                newResultSelector.Body.Type);

            var rewritten = Expression.Call(
                newMethodInfo,
                outerResult.source,
                innerResult.source,
                outerResult.lambda,
                innerResult.lambda,
                newResultSelector);

            var transparentIdentifierParameter = Expression.Parameter(resultType, "ti");

            var newNavigationExpansionMapping = new List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)>();


            foreach (var outerMappingEntry in outerResult.state.SourceMappings)
            {
                foreach (var outerTransparentIdentifierMapping in outerMappingEntry.TransparentIdentifierMapping)
                {
                    outerTransparentIdentifierMapping.path.Insert(0, "Outer");
                }

                //outerMappingEntry.InitialPath.Insert(0, "Outer");
            }

            foreach (var innerMappingEntry in innerResult.state.SourceMappings)
            {
                foreach (var innerTransparentIdentifierMapping in innerMappingEntry.TransparentIdentifierMapping)
                {
                    innerTransparentIdentifierMapping.path.Insert(0, "Inner");
                }

                //innerMappingEntry.InitialPath.Insert(0, "Inner");
            }

            //foreach (var outerMappingEntry in outerResult.state.NavigationExpansionMapping)
            //{
            //    newNavigationExpansionMapping.Add((
            //        path: new[] { "Outer" }.Concat(outerMappingEntry.path).ToList(),
            //        initialPath: new[] { "Outer" }.Concat(outerMappingEntry.initialPath).ToList(),
            //        outerMappingEntry.rootEntityType,
            //        outerMappingEntry.navigations ));
            //}

            //foreach (var innerMappingEntry in innerResult.state.NavigationExpansionMapping)
            //{
            //    newNavigationExpansionMapping.Add((
            //        path: new[] { "Inner" }.Concat(innerMappingEntry.path).ToList(),
            //        initialPath: new[] { "Inner" }.Concat(innerMappingEntry.initialPath).ToList(),
            //        innerMappingEntry.rootEntityType,
            //        innerMappingEntry.navigations));
            //}

            //foreach (var outerMappingEntry in outerResult.state.TransparentIdentifierAccessorMapping)
            //{
            //    newTransparentIdentifierAccessorMapping.Add((outerMappingEntry.from, to: new[] { "Outer" }.Concat(outerMappingEntry.to).ToList()));
            //}

            //foreach (var innerMappingEntry in innerResult.state.TransparentIdentifierAccessorMapping)
            //{
            //    newTransparentIdentifierAccessorMapping.Add((innerMappingEntry.from, to: new[] { "Inner" }.Concat(innerMappingEntry.to).ToList()));
            //}

            var outerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Outer));
            var innerAccess = Expression.Field(transparentIdentifierParameter, nameof(TransparentIdentifier<object, object>.Inner));

            var foo = ExpressionExtensions.CombineAndRemapLambdas(outerResult.state.PendingSelector, resultSelector, resultSelector.Parameters[0]);
            var foo2 = ExpressionExtensions.CombineAndRemapLambdas(innerResult.state.PendingSelector, foo, resultSelector.Parameters[1]);

            var foo3 = new ExpressionReplacingVisitor(outerResult.state.CurrentParameter, outerAccess).Visit(foo2.Body);
            var foo4 = new ExpressionReplacingVisitor(innerResult.state.CurrentParameter, innerAccess).Visit(foo3);

            var lambda = Expression.Lambda(foo4, transparentIdentifierParameter);

            var select = QueryableSelectMethodInfo.MakeGenericMethod(transparentIdentifierParameter.Type, lambda.Body.Type);

            var finalState = new NavigationExpansionExpressionState
            {
                PendingSelector = lambda,
                CurrentParameter = transparentIdentifierParameter,
                FinalProjectionPath = new List<string>(),
                SourceMappings = outerResult.state.SourceMappings.Concat(innerResult.state.SourceMappings).ToList()

                //FoundNavigations = outerResult.state.FoundNavigations.Concat(innerResult.state.FoundNavigations).ToList(), // ???
                //NavigationExpansionMapping = newNavigationExpansionMapping
                //TransparentIdentifierAccessorMapping = newTransparentIdentifierAccessorMapping
            };

            var fubar = new NavigationExpansionExpression(
                rewritten,
                finalState,
                select.ReturnType);
            //rewritten.Type);

            var fubar22 = FindAndApplyNavigations(rewritten, lambda, finalState);



            fubar22.state.PendingSelector = (LambdaExpression)fubar22.lambda;

            return new NavigationExpansionExpression(
                fubar22.source,
                fubar22.state,
                select.ReturnType);


            //var finalResult = new NavigationExpansionExpression(fubar22.source, fubar22.state, select.ReturnType);


            //return finalResult;

            //return fubar;





            //var fubaz = Expression.Call(select, fubar, lambda);

            //return Visit(fubaz);


            //// append Select method representing the result selector of the Join operation





            //return methodCallExpression;
        }

        private class ExpressionReplacingVisitor  : ExpressionVisitor
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

        //private Expression ProcessJoin(MethodCallExpression methodCallExpression)
        //{
        //    var outerSource = Visit(methodCallExpression.Arguments[0]);
        //    var innerSource = Visit(methodCallExpression.Arguments[1]);

        //    var outerKeySelector = methodCallExpression.Arguments[2].UnwrapQuote();
        //    var innerKeySelector = methodCallExpression.Arguments[3].UnwrapQuote();
        //    var resultSelector = methodCallExpression.Arguments[4].UnwrapQuote();

        //    var outerState = new NavigationExpansionExpressionState
        //    {
        //        CurrentParameter = outerKeySelector.Parameters[0]
        //    };

        //    var innerState = new NavigationExpansionExpressionState
        //    {
        //        CurrentParameter = innerKeySelector.Parameters[0]
        //    };

        //    if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
        //    {
        //        outerSource = outerNavigationExpansionExpression.Operand;

        //        // TODO: fix this!
        //        var currentParameter = outerState.CurrentParameter;
        //        outerState = outerNavigationExpansionExpression.State;
        //        outerState.CurrentParameter = outerState.CurrentParameter ?? currentParameter;
        //        outerState.PendingSelector = outerState.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
        //    }

        //    if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
        //    {
        //        innerSource = innerNavigationExpansionExpression.Operand;

        //        // TODO: fix this!
        //        var currentParameter = innerState.CurrentParameter;
        //        innerState = innerNavigationExpansionExpression.State;
        //        innerState.CurrentParameter = innerState.CurrentParameter ?? currentParameter;
        //        innerState.PendingSelector = innerState.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
        //    }

        //    var combinedOuterKeySelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, outerKeySelector);
        //    combinedOuterKeySelector = (LambdaExpression)Visit(combinedOuterKeySelector);

        //    var combinedInnerKeySelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, innerKeySelector);
        //    combinedInnerKeySelector = (LambdaExpression)Visit(combinedInnerKeySelector);

        //    var combinedResultSelector = ExpressionExtensions.CombineAndRemapLambdas(outerState.PendingSelector, resultSelector, resultSelector.Parameters[0]);
        //    combinedResultSelector = ExpressionExtensions.CombineAndRemapLambdas(innerState.PendingSelector, combinedResultSelector, combinedResultSelector.Parameters[1]);

        //    var outerBinder = new NavigationPropertyBindingExpressionVisitor2(
        //        outerState.CurrentParameter,
        //        outerState.TransparentIdentifierAccessorMapping,
        //        outerState.EntityTypeAccessorMapping);

        //    var innerBinder = new NavigationPropertyBindingExpressionVisitor2(
        //        innerState.CurrentParameter,
        //        innerState.TransparentIdentifierAccessorMapping,
        //        innerState.EntityTypeAccessorMapping);

        //    var boundOuterKeySelector = outerBinder.Visit(combinedOuterKeySelector);
        //    var boundInnerKeySelector = innerBinder.Visit(combinedInnerKeySelector);
        //    var boundResultSelector = outerBinder.Visit(combinedResultSelector);
        //    boundResultSelector = innerBinder.Visit(boundResultSelector);

        //    var outerCnrev = new CollectionNavigationRewritingExpressionVisitor2(outerState.CurrentParameter);
        //    var innerCnrev = new CollectionNavigationRewritingExpressionVisitor2(innerState.CurrentParameter);

        //    boundOuterKeySelector = (LambdaExpression)outerCnrev.Visit(boundOuterKeySelector);
        //    boundInnerKeySelector = (LambdaExpression)innerCnrev.Visit(boundInnerKeySelector);
        //    boundResultSelector = (LambdaExpression)outerCnrev.Visit(boundResultSelector);
        //    boundResultSelector = (LambdaExpression)innerCnrev.Visit(boundResultSelector);

        //    var outerNfev = new NavigationFindingExpressionVisitor2(outerState.CurrentParameter, outerState.FoundNavigations);
        //    var innerNfev = new NavigationFindingExpressionVisitor2(innerState.CurrentParameter, innerState.FoundNavigations);

        //    outerNfev.Visit(boundOuterKeySelector);
        //    innerNfev.Visit(boundInnerKeySelector);
        //    outerNfev.Visit(boundResultSelector);
        //    innerNfev.Visit(boundResultSelector);

        //    var outerResult = (source: outerSource, parameter: outerState.CurrentParameter, pendingSelector: outerState.PendingSelector);
        //    if (outerNfev.FoundNavigations.Any())
        //    {
        //        foreach (var navigationTree in outerNfev.FoundNavigations)
        //        {
        //            outerResult = AddNavigationJoin(
        //                outerResult.source,
        //                outerResult.parameter,
        //                navigationTree,
        //                new List<INavigation>(),
        //                outerState.FinalProjectionPath,
        //                outerState.TransparentIdentifierAccessorMapping,
        //                outerState.PendingSelector);
        //        }
        //    }

        //    var innerResult = (source: innerSource, parameter: innerState.CurrentParameter, pendingSelector: innerState.PendingSelector);
        //    if (innerNfev.FoundNavigations.Any())
        //    {
        //        foreach (var navigationTree in innerNfev.FoundNavigations)
        //        {
        //            innerResult = AddNavigationJoin(
        //                innerResult.source,
        //                innerResult.parameter,
        //                navigationTree,
        //                new List<INavigation>(),
        //                innerState.FinalProjectionPath,
        //                innerState.TransparentIdentifierAccessorMapping,
        //                innerState.PendingSelector);
        //        }
        //    }

        //    var outerNrev = new NavigationReplacingExpressionVisitor2(
        //        outerState.CurrentParameter,
        //        outerResult.parameter,
        //        outerState.TransparentIdentifierAccessorMapping);

        //    var innerNrev = new NavigationReplacingExpressionVisitor2(
        //        innerState.CurrentParameter,
        //        innerResult.parameter,
        //        innerState.TransparentIdentifierAccessorMapping);

        //    var newOuterKeySelector = outerNrev.Visit(boundOuterKeySelector);
        //    var newInnerKeySelector = innerNrev.Visit(boundInnerKeySelector);
        //    var newResultSelector = outerNrev.Visit(boundResultSelector);
        //    newResultSelector = innerNrev.Visit(newResultSelector);

        //    var newOuterState = new NavigationExpansionExpressionState
        //    {
        //        CurrentParameter = outerResult.parameter,
        //        EntityTypeAccessorMapping = outerState.EntityTypeAccessorMapping,
        //        FinalProjectionPath = outerState.FinalProjectionPath,
        //        FoundNavigations = outerState.FoundNavigations,
        //        PendingSelector = outerResult.pendingSelector,
        //        TransparentIdentifierAccessorMapping = outerState.TransparentIdentifierAccessorMapping,
        //    };

        //    var newInnerState = new NavigationExpansionExpressionState
        //    {
        //        CurrentParameter = innerResult.parameter,
        //        EntityTypeAccessorMapping = innerState.EntityTypeAccessorMapping,
        //        FinalProjectionPath = innerState.FinalProjectionPath,
        //        FoundNavigations = innerState.FoundNavigations,
        //        PendingSelector = innerResult.pendingSelector,
        //        TransparentIdentifierAccessorMapping = innerState.TransparentIdentifierAccessorMapping,
        //    };

        //    var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
        //        newOuterState.CurrentParameter.Type,
        //        newInnerState.CurrentParameter.Type,
        //        newOuterKeySelector.UnwrapQuote().Body.Type,
        //        newResultSelector.UnwrapQuote().Body.Type);

        //    var rewritten = Expression.Call(
        //        newMethodInfo,
        //        outerResult.source,
        //        innerResult.source,
        //        newOuterKeySelector,
        //        newInnerKeySelector,
        //        newResultSelector);

        //    newOuterState.PendingSelector = null;
        //    newOuterState.FinalProjectionPath = new List<string>();

        //    return new NavigationExpansionExpression(
        //        rewritten,
        //        newOuterState,//TODO: rong
        //        methodCallExpression.Type);
        //}

        private Expression ProcessGroupJoin(MethodCallExpression methodCallExpression)
        {
            return methodCallExpression;

            //var outerSource = Visit(methodCallExpression.Arguments[0]);
            //var innerSource = Visit(methodCallExpression.Arguments[1]);

            //var outerKeySelector = methodCallExpression.Arguments[2];
            //var innerKeySelector = methodCallExpression.Arguments[3];
            //var resultSelector = methodCallExpression.Arguments[4];

            //var outerKeySelectorCurrentParameter = outerKeySelector.UnwrapQuote().Parameters[0];
            //var innerKeySelectorCurrentParameter = innerKeySelector.UnwrapQuote().Parameters[0];
            //var firstResultSelectorCurrentParameter = resultSelector.UnwrapQuote().Parameters[0];
            //var secondResultSelectorCurrentParameter = resultSelector.UnwrapQuote().Parameters[1];

            //// TODO: shouldnt those be null?????
            //var outerFirstSelectorParameter = outerKeySelectorCurrentParameter;
            //var innerFirstSelectorParameter = innerKeySelectorCurrentParameter;

            //var outerKeySelectorOriginalParameter = outerKeySelectorCurrentParameter;
            //var innerKeySelectorOriginalParameter = innerKeySelectorCurrentParameter;
            //var firstResultSelectorOriginalParameter = firstResultSelectorCurrentParameter;
            //var secondResultSelectorOriginalParameter = secondResultSelectorCurrentParameter;

            //var outerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            //var outerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            //var outerPendingSelector = default(LambdaExpression);
            //var outerFoundNavigations = new List<NavigationTreeNode>();
            //var outerFinalProjectionPath = new List<string>();

            //var innerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            //var innerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            //var innerPendingSelector = default(LambdaExpression);
            //var innerFoundNavigations = new List<NavigationTreeNode>();
            //var innerFinalProjectionPath = new List<string>();

            //if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            //{
            //    outerSource = outerNavigationExpansionExpression.Operand;

            //    //outerFirstSelectorParameter = outerNavigationExpansionExpression.FirstSelectorParameter ?? outerFirstSelectorParameter;
            //    //outerKeySelectorCurrentParameter = outerNavigationExpansionExpression.CurrentParameter ?? outerKeySelectorCurrentParameter;

            //    outerTransparentIdentifierAccessorMapping = outerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
            //    outerEntityTypeAccessorMapping = outerNavigationExpansionExpression.EntityTypeAccessorMapping;

            //    outerPendingSelector = outerNavigationExpansionExpression.PendingSelector;

            //    outerFoundNavigations = outerNavigationExpansionExpression.FoundNavigations;
            //    outerFinalProjectionPath = outerNavigationExpansionExpression.FinalProjectionPath;
            //}

            //if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            //{
            //    innerSource = innerNavigationExpansionExpression.Operand;

            //    //innerFirstSelectorParameter = innerNavigationExpansionExpression.FirstSelectorParameter ?? innerFirstSelectorParameter;
            //    //innerKeySelectorCurrentParameter = innerNavigationExpansionExpression.CurrentParameter ?? innerKeySelectorCurrentParameter;

            //    innerTransparentIdentifierAccessorMapping = innerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
            //    innerEntityTypeAccessorMapping = innerNavigationExpansionExpression.EntityTypeAccessorMapping;

            //    innerPendingSelector = innerNavigationExpansionExpression.PendingSelector;

            //    innerFoundNavigations = innerNavigationExpansionExpression.FoundNavigations;
            //    innerFinalProjectionPath = innerNavigationExpansionExpression.FinalProjectionPath;
            //}

            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            //var compensatedOuterKeySelector = ExpressionExtensions.CombineLambdas(outerPendingSelector, outerKeySelector.UnwrapQuote());

            ////var outerCnrev = new CollectionNavigationRewritingExpressionVisitor(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            ////compensatedOuterKeySelector = (LambdaExpression)outerCnrev.Visit(compensatedOuterKeySelector);

            //var outerBinder = new NavigationPropertyBindingExpressionVisitor2(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping);
            //var boundOuterKeySelector = (LambdaExpression)outerBinder.Visit(compensatedOuterKeySelector);

            //var outerCnrev = new CollectionNavigationRewritingExpressionVisitor2(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter);
            //compensatedOuterKeySelector = (LambdaExpression)outerCnrev.Visit(boundOuterKeySelector);

            //var outerNfev = new NavigationFindingExpressionVisitor2(outerKeySelectorCurrentParameter, outerFoundNavigations);
            //outerNfev.Visit(compensatedOuterKeySelector);







            ////var outerNfev = new NavigationFindingExpressionVisitor(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            ////outerNfev.Visit(compensatedOuterKeySelector);





            //var compensatedInnerKeySelector = ExpressionExtensions.CombineLambdas(innerPendingSelector, innerKeySelector.UnwrapQuote());

            //var innerBinder = new NavigationPropertyBindingExpressionVisitor2(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping);
            //var boundInnerKeySelector = innerBinder.Visit(compensatedInnerKeySelector);

            //var innerCnrev = new CollectionNavigationRewritingExpressionVisitor2(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter);
            //compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(boundInnerKeySelector);

            ////var innerCnrev = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(compensatedInnerKeySelector);

            ////var innerNfev = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////innerNfev.Visit(compensatedInnerKeySelector);

            //var innerNfev = new NavigationFindingExpressionVisitor2(innerKeySelectorCurrentParameter, innerFoundNavigations);
            //innerNfev.Visit(compensatedInnerKeySelector);





            //var compensatedResultSelector = ExpressionExtensions.CombineLambdas(outerPendingSelector, resultSelector.UnwrapQuote());
            //compensatedResultSelector = ExpressionExtensions.CombineLambdas(innerPendingSelector, compensatedResultSelector);




            ////var resultCnrev1 = new CollectionNavigationRewritingExpressionVisitor(outerFirstSelectorParameter ?? firstResultSelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            ////compensatedResultSelector = (LambdaExpression)resultCnrev1.Visit(compensatedResultSelector);

            ////var resultCnrev2 = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////compensatedResultSelector = (LambdaExpression)resultCnrev2.Visit(compensatedResultSelector);



            ////var innerNfev1 = new NavigationFindingExpressionVisitor(outerFirstSelectorParameter ?? firstResultSelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            ////innerNfev1.Visit(compensatedInnerKeySelector);

            ////var innerNfev2 = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////innerNfev2.Visit(compensatedInnerKeySelector);

            //var innerNfev1 = new NavigationFindingExpressionVisitor2(outerKeySelectorCurrentParameter, outerFoundNavigations);
            //innerNfev1.Visit(compensatedInnerKeySelector);

            //var innerNfev2 = new NavigationFindingExpressionVisitor2(innerKeySelectorCurrentParameter, innerFoundNavigations);
            //innerNfev2.Visit(compensatedInnerKeySelector);





            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            //var outerResult = (source: outerSource, parameter: outerKeySelectorCurrentParameter, pendingSelector: outerPendingSelector);

            //if (outerFoundNavigations.Any())
            ////if (outerNfev.FoundNavigationPaths.Any())
            //{
            //    foreach (var navigationPath in outerFoundNavigations)
            //    //foreach (var navigationPath in outerNfev.FoundNavigationPaths)
            //    {
            //        outerResult = AddNavigationJoin(
            //            outerResult.source,
            //            outerResult.parameter,
            //            navigationPath,
            //            new List<INavigation>(),
            //            outerFinalProjectionPath,
            //            outerTransparentIdentifierAccessorMapping,
            //            outerPendingSelector);
            //    }
            //}

            //var newOuterSource = outerResult.source;
            //var outerKeySelectorPreviousParameter = outerKeySelectorCurrentParameter;
            //outerKeySelectorCurrentParameter = outerResult.parameter;
            //firstResultSelectorCurrentParameter = outerResult.parameter;
            //outerPendingSelector = outerResult.pendingSelector;

            ////// here we need 3 parameters:
            ////// - original parameter: parameter in the original lambda. It will be present in the parameter list since pending selector compensation only modified the body.
            ////// - root parameter: parameter representing root on the navigation chain. It was injected to the lambda after pending selector compensation.
            ////// - current parameter: parameter representing current transparent identifier. All parameter expressions should be replaced to it
            ////var outerNrev = new NavigationReplacingExpressionVisitor(
            ////    _model,
            ////    outerKeySelectorOriginalParameter,
            ////    outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter,//  rootParameter, <--- TODO: is this correct???????
            ////    outerKeySelectorCurrentParameter,
            ////    outerTransparentIdentifierAccessorMapping,
            ////    outerEntityTypeAccessorMapping);

            //var outerNrev = new NavigationReplacingExpressionVisitor2(
            //    outerKeySelectorPreviousParameter,
            //    outerKeySelectorCurrentParameter,
            //    outerTransparentIdentifierAccessorMapping);

            //var newOuterKeySelector = outerNrev.Visit(compensatedOuterKeySelector);

            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            ////var innerCombinedSelector = ExpressionExtensions.CombineLambdas(innerAppliedSelector, innerPendingSelector);
            ////var compensatedInnerKeySelector = ExpressionExtensions.CombineLambdas(innerCombinedSelector, innerKeySelector.UnwrapQuote());

            ////var innerCnrev = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(compensatedInnerKeySelector);

            ////var innerNfev = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerEntityTypeAccessorMapping, innerFoundNavigations);
            ////innerNfev.Visit(compensatedInnerKeySelector);

            //var innerResult = (source: innerSource, parameter: innerKeySelectorCurrentParameter, pendingSelector: innerPendingSelector);
            //if (innerFoundNavigations.Any())
            ////if (innerNfev.FoundNavigationPaths.Any())
            //{
            //    foreach (var navigationPath in innerFoundNavigations)
            //    //foreach (var navigationPath in innerNfev.FoundNavigationPaths)
            //    {
            //        innerResult = AddNavigationJoin(
            //            innerResult.source,
            //            innerResult.parameter,
            //            navigationPath,
            //            new List<INavigation>(),
            //            innerFinalProjectionPath,
            //            innerTransparentIdentifierAccessorMapping,
            //            innerPendingSelector);
            //    }
            //}

            //var newInnerSource = innerResult.source;
            //var innerKeySelectorPreviousParameter = innerKeySelectorCurrentParameter;
            //innerKeySelectorCurrentParameter = innerResult.parameter;
            //innerPendingSelector = innerResult.pendingSelector;

            ////// PROBABLY WRONG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            ////secondResultSelectorCurrentParameter = innerResult.parameter;


            //// here we need 3 parameters:
            //// - original parameter: parameter in the original lambda. It will be present in the parameter list since pending selector compensation only modified the body.
            //// - root parameter: parameter representing root on the navigation chain. It was injected to the lambda after pending selector compensation.
            //// - current parameter: parameter representing current transparent identifier. All parameter expressions should be replaced to it
            //var innerNrev = new NavigationReplacingExpressionVisitor2(
            //    innerKeySelectorPreviousParameter,
            //    innerKeySelectorCurrentParameter,
            //    innerTransparentIdentifierAccessorMapping);

            //var newInnerKeySelector = innerNrev.Visit(compensatedInnerKeySelector);




            //var firstResultNrev = new NavigationReplacingExpressionVisitor2(
            //    firstResultSelectorOriginalParameter,
            //    firstResultSelectorCurrentParameter,
            //    outerTransparentIdentifierAccessorMapping);

            //var newResultSelector = firstResultNrev.Visit(compensatedResultSelector);


            ////// TODO: fix the second argument - need to convert to collection (somehow)
            ////var secondResultNrev = new NavigationReplacingExpressionVisitor(
            ////    _model,
            ////    secondResultSelectorOriginalParameter,
            ////    innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter,
            ////    secondResultSelectorCurrentParameter,
            ////    innerTransparentIdentifierAccessorMapping,
            ////    innerEntityTypeAccessorMapping);

            ////newResultSelector = secondResultNrev.Visit(newResultSelector);






            //// TODO:
            ////
            //// WHAT DO WE DO ABOUT RESULT SELECTOR????? - does join/groupjoin need to be "terminating operation"??
            //// or is there some smart way to delay the selector

            //var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
            //    outerResult.parameter.Type,
            //    innerResult.parameter.Type,
            //    outerKeySelector.UnwrapQuote().Body.Type,
            //    resultSelector.UnwrapQuote().Body.Type);

            ////var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.parameter.Type);
            //var rewritten = Expression.Call(newMethodInfo, newOuterSource, newInnerSource, newOuterKeySelector, newInnerKeySelector, newResultSelector);

            //// i guess we need to reset - no way to fit both sources here otherwise


            //return rewritten;

            ////return new NavigationExpansionExpression(
            ////    rewritten,
            ////    firstSelectorParameter,
            ////    result.parameter,
            ////    transparentIdentifierAccessorMapping,
            ////    entityTypeAccessorMapping,
            ////    appliedSelector,
            ////    pendingSelector,
            ////    foundNavigations,
            ////    finalProjectionPath,
            ////    methodCallExpression.Type);
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

                // TODO: fix this!
                var currentParameter = state.CurrentParameter;
                state = navigationExpansionExpression.State;
                state.CurrentParameter = state.CurrentParameter ?? currentParameter;

                if (state.PendingSelector != null)
                {
                    var pendingSelectorParameter = state.PendingSelector.Parameters[0];

                    var binder = new NavigationPropertyBindingExpressionVisitor2(
                        pendingSelectorParameter,
                        state.SourceMappings);//state.NavigationExpansionMapping);

                    var boundSelector = binder.Visit(state.PendingSelector);

                    var nrev = new NavigationReplacingExpressionVisitor2(
                        pendingSelectorParameter,
                        pendingSelectorParameter);

                    var newSelector = nrev.Visit(boundSelector);

                    var etamg = new EntityTypeAccessorMappingGenerator2(pendingSelectorParameter);
                    etamg.Visit(boundSelector);

                    var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                        pendingSelectorParameter.Type,
                        ((LambdaExpression)newSelector).Body.Type);

                    var result = Expression.Call(selectorMethodInfo, navigationExpansionExpression.Operand, newSelector);

                    state.PendingSelector = null;
                    state.CurrentParameter = null;

                    if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                        || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny))
                    {
                        source = methodCallExpression.Update(methodCallExpression.Object, new[] { result });
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

                    //////////var etamg = new EntityTypeAccessorMappingGenerator(
                    //////////    pendingSelectorParameter,
                    //////////    state.NavigationExpansionMapping);

                    //////////etamg.Visit(boundSelector);

                    //////////return new NavigationExpansionExpression(
                    //////////    source,
                    //////////    new NavigationExpansionExpressionState() { NavigationExpansionMapping = etamg.NewNavigationExpansionMapping },
                    //////////    methodCallExpression.Type);
                }
                else
                {
                    // TODO: need to run thru Expression.Update?
                    source = methodCallExpression;
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
                    },
                    constantExpression.Type);

                return result;
            }
           
            return base.VisitConstant(constantExpression);
        }

        private (Expression source, Expression lambda, NavigationExpansionExpressionState state) FindAndApplyNavigations(
            Expression source,
            LambdaExpression lambda,
            NavigationExpansionExpressionState state)
        {
            var binder = new NavigationPropertyBindingExpressionVisitor2(
                state.CurrentParameter,
                state.SourceMappings);

            var boundLambda = binder.Visit(lambda);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(state.CurrentParameter);
            boundLambda = (LambdaExpression)cnrev.Visit(boundLambda);

            var nfev = new NavigationFindingExpressionVisitor2(state.CurrentParameter);
            nfev.Visit(boundLambda);

            var result = (source, parameter: state.CurrentParameter, pendingSelector: state.PendingSelector);
            foreach (var sourceMapping in state.SourceMappings)
            {
                if (sourceMapping.FoundNavigations.Any())
                {
                    foreach (var navigationTree in sourceMapping.FoundNavigations)
                    {
                        result = AddNavigationJoin2(
                            result.source,
                            result.parameter,
                            sourceMapping,
                            state.SourceMappings,
                            navigationTree,
                            new List<INavigation>(),
                            result.pendingSelector);
                            //state.PendingSelector);
                    }
                }
            }

            var nrev = new NavigationReplacingExpressionVisitor2(
                state.CurrentParameter,
                result.parameter);

            var newLambda = nrev.Visit(boundLambda);

            var newState = new NavigationExpansionExpressionState
            {
                CurrentParameter = result.parameter,
                SourceMappings = state.SourceMappings,
                FinalProjectionPath = state.FinalProjectionPath,
                PendingSelector = result.pendingSelector,
            };

            return (result.source, lambda: newLambda, state: newState);
        }

        //private (Expression source, ParameterExpression parameter, LambdaExpression pendingSelector) AddNavigationJoin(
        //    Expression sourceExpression,
        //    ParameterExpression parameterExpression,
        //    NavigationTreeNode navigationTree,
        //    IEntityType rootEntityType,
        //    List<INavigation> navigationPath,
        //    List<string> finalProjectionPath,
        //    List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)> navigationExpansionMapping,
        //    LambdaExpression pendingSelector)
        //{
        //    var path = navigationTree.GeneratePath();

        //    if (!navigationExpansionMapping.Any(m => m.navigations.Count == path.Count && m.navigations.Zip(path, (o, i) => o.Name == i).All(r => r)))
        //    {
        //        var navigation = navigationTree.Navigation;
        //        var sourceType = sourceExpression.Type.GetGenericArguments()[0];

        //        // is this the right way to get EntityTypes?
        //        var navigationTargetEntityType = navigation.IsDependentToPrincipal()
        //            ? navigation.ForeignKey.PrincipalEntityType
        //            : navigation.ForeignKey.DeclaringEntityType;

        //        var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
        //        var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

        //        var transparentIdentifierAccessorPath = navigationExpansionMapping.Where(
        //            m => m.navigations.Count == navigationPath.Count
        //                && m.navigations.Zip(navigationPath, (o, i) => o == i).All(r => r)).SingleOrDefault().path;

        //        var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
        //        var outerKeySelectorParameter = outerParameter;
        //        var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, transparentIdentifierAccessorPath);

        //        var outerKeySelectorBody = CreateKeyAccessExpression(
        //            transparentIdentifierAccessorExpression,
        //            navigation.IsDependentToPrincipal()
        //                ? navigation.ForeignKey.Properties
        //                : navigation.ForeignKey.PrincipalKey.Properties,
        //            addNullCheck: navigationTree.Parent != null && navigationTree.Parent.Optional);

        //        var innerKeySelectorParameterType = navigationTargetEntityType.ClrType;
        //        var innerKeySelectorParameter = Expression.Parameter(
        //            innerKeySelectorParameterType,
        //            parameterExpression.Name + "." + navigationTree.Navigation.Name);

        //        var innerKeySelectorBody = CreateKeyAccessExpression(
        //            innerKeySelectorParameter,
        //            navigation.IsDependentToPrincipal()
        //                ? navigation.ForeignKey.PrincipalKey.Properties
        //                : navigation.ForeignKey.Properties);

        //        if (outerKeySelectorBody.Type.IsNullableType()
        //            && !innerKeySelectorBody.Type.IsNullableType())
        //        {
        //            innerKeySelectorBody = Expression.Convert(innerKeySelectorBody, outerKeySelectorBody.Type);
        //        }
        //        else if (innerKeySelectorBody.Type.IsNullableType()
        //            && !outerKeySelectorBody.Type.IsNullableType())
        //        {
        //            outerKeySelectorBody = Expression.Convert(outerKeySelectorBody, innerKeySelectorBody.Type);
        //        }

        //        var outerKeySelector = Expression.Lambda(
        //            outerKeySelectorBody,
        //            outerKeySelectorParameter);

        //        var innerKeySelector = Expression.Lambda(
        //            innerKeySelectorBody,
        //            innerKeySelectorParameter);

        //        var oldParameterExpression = parameterExpression;
        //        if (navigationTree.Optional)
        //        {
        //            var groupingType = typeof(IEnumerable<>).MakeGenericType(navigationTargetEntityType.ClrType);
        //            var groupJoinResultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, groupingType);

        //            var groupJoinMethodInfo = QueryableGroupJoinMethodInfo.MakeGenericMethod(
        //                sourceType,
        //                navigationTargetEntityType.ClrType,
        //                outerKeySelector.Body.Type,
        //                groupJoinResultType);

        //            var resultSelectorOuterParameterName = outerParameter.Name;
        //            var resultSelectorOuterParameter = Expression.Parameter(sourceType, resultSelectorOuterParameterName);

        //            var resultSelectorInnerParameterName = innerKeySelectorParameter.Name;
        //            var resultSelectorInnerParameter = Expression.Parameter(groupingType, resultSelectorInnerParameterName);

        //            var groupJoinResultTransparentIdentifierCtorInfo
        //                = groupJoinResultType.GetTypeInfo().GetConstructors().Single();

        //            var groupJoinResultSelector = Expression.Lambda(
        //                Expression.New(groupJoinResultTransparentIdentifierCtorInfo, resultSelectorOuterParameter, resultSelectorInnerParameter),
        //                resultSelectorOuterParameter,
        //                resultSelectorInnerParameter);

        //            var groupJoinMethodCall
        //                = Expression.Call(
        //                    groupJoinMethodInfo,
        //                    sourceExpression,
        //                    entityQueryable,
        //                    outerKeySelector,
        //                    innerKeySelector,
        //                    groupJoinResultSelector);

        //            var selectManyResultType = typeof(TransparentIdentifier<,>).MakeGenericType(groupJoinResultType, navigationTargetEntityType.ClrType);

        //            var selectManyMethodInfo = QueryableSelectManyWithResultOperatorMethodInfo.MakeGenericMethod(
        //                groupJoinResultType,
        //                navigationTargetEntityType.ClrType,
        //                selectManyResultType);

        //            var defaultIfEmptyMethodInfo = EnumerableDefaultIfEmptyMethodInfo.MakeGenericMethod(navigationTargetEntityType.ClrType);

        //            var selectManyCollectionSelectorParameter = Expression.Parameter(groupJoinResultType);
        //            var selectManyCollectionSelector = Expression.Lambda(
        //                Expression.Call(
        //                    defaultIfEmptyMethodInfo,
        //                    Expression.Field(selectManyCollectionSelectorParameter, nameof(TransparentIdentifier<object, object>.Inner))),
        //                selectManyCollectionSelectorParameter);

        //            var selectManyResultTransparentIdentifierCtorInfo
        //                = selectManyResultType.GetTypeInfo().GetConstructors().Single();

        //            // TODO: dont reuse parameters here?
        //            var selectManyResultSelector = Expression.Lambda(
        //                Expression.New(selectManyResultTransparentIdentifierCtorInfo, selectManyCollectionSelectorParameter, innerKeySelectorParameter),
        //                selectManyCollectionSelectorParameter,
        //                innerKeySelectorParameter);

        //            var selectManyMethodCall
        //                = Expression.Call(selectManyMethodInfo,
        //                groupJoinMethodCall,
        //                selectManyCollectionSelector,
        //                selectManyResultSelector);

        //            sourceType = selectManyResultSelector.ReturnType;
        //            sourceExpression = selectManyMethodCall;

        //            var transparentIdentifierParameterName = resultSelectorInnerParameterName;
        //            var transparentIdentifierParameter = Expression.Parameter(selectManyResultSelector.ReturnType, transparentIdentifierParameterName);
        //            parameterExpression = transparentIdentifierParameter;
        //        }
        //        else
        //        {
        //            var joinMethodInfo = QueryableJoinMethodInfo.MakeGenericMethod(
        //                sourceType,
        //                navigationTargetEntityType.ClrType,
        //                outerKeySelector.Body.Type,
        //                resultType);

        //            var resultSelectorOuterParameterName = outerParameter.Name;
        //            var resultSelectorOuterParameter = Expression.Parameter(sourceType, resultSelectorOuterParameterName);

        //            var resultSelectorInnerParameterName = innerKeySelectorParameter.Name;
        //            var resultSelectorInnerParameter = Expression.Parameter(navigationTargetEntityType.ClrType, resultSelectorInnerParameterName);

        //            var transparentIdentifierCtorInfo
        //                = resultType.GetTypeInfo().GetConstructors().Single();

        //            var resultSelector = Expression.Lambda(
        //                Expression.New(transparentIdentifierCtorInfo, resultSelectorOuterParameter, resultSelectorInnerParameter),
        //                resultSelectorOuterParameter,
        //                resultSelectorInnerParameter);

        //            var joinMethodCall = Expression.Call(
        //                joinMethodInfo,
        //                sourceExpression,
        //                entityQueryable,
        //                outerKeySelector,
        //                innerKeySelector,
        //                resultSelector);

        //            sourceType = resultSelector.ReturnType;
        //            sourceExpression = joinMethodCall;

        //            var transparentIdentifierParameterName = /*resultSelectorOuterParameterName + */resultSelectorInnerParameterName;
        //            var transparentIdentifierParameter = Expression.Parameter(resultSelector.ReturnType, transparentIdentifierParameterName);
        //            parameterExpression = transparentIdentifierParameter;
        //        }

        //        if (navigationPath.Count == 0
        //            && !navigationExpansionMapping.Any(m => m.navigations.Count == 0))
        //        {
        //            navigationExpansionMapping.Add((path: new List<string>(), initialPath: new List<string>(), rootEntityType, navigations: navigationPath.ToList()));
        //        }

        //        foreach (var navigationExpansionMappingElement in navigationExpansionMapping)
        //        {
        //            navigationExpansionMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));

        //            // in case of GroupJoin (optional navigation) source is hidden deeps since we also project the grouping
        //            // we could remove the grouping in the future, but for nowe we need the grouping to properly recognize the LOJ pattern
        //            if (navigationTree.Optional)
        //            {
        //                navigationExpansionMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
        //            }
        //        }

        //        navigationPath.Add(navigation);
        //        navigationExpansionMapping.Add((path: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }, initialPath: new List<string>(), rootEntityType, navigations: navigationPath.ToList() ));

        //        finalProjectionPath.Add("Outer");
        //        if (navigationTree.Optional)
        //        {
        //            finalProjectionPath.Add("Outer");
        //        }

        //        if (pendingSelector != null)
        //        {
        //            var psuev = new PendingSelectorUpdatingExpressionVisitor(oldParameterExpression, parameterExpression, navigationTree.Optional);
        //            pendingSelector = (LambdaExpression)psuev.Visit(pendingSelector);
        //        }
        //    }
        //    else
        //    {
        //        navigationPath.Add(navigationTree.Navigation);
        //    }

        //    var result = (source: sourceExpression, parameter: parameterExpression, pendingSelector);
        //    foreach (var child in navigationTree.Children)
        //    {
        //        result = AddNavigationJoin(
        //            result.source,
        //            result.parameter,
        //            child,
        //            rootEntityType,
        //            navigationPath.ToList(),
        //            finalProjectionPath,
        //            navigationExpansionMapping,
        //            result.pendingSelector);
        //    }

        //    return result;
        //}

        private (Expression source, ParameterExpression parameter, LambdaExpression pendingSelector) AddNavigationJoin2(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            SourceMapping sourceMapping,
            List<SourceMapping> allSourceMappings,
            NavigationTreeNode navigationTree,
            List<INavigation> navigationPath,
            LambdaExpression pendingSelector)
        {
            var path = navigationTree.GeneratePath();
            if (!sourceMapping.TransparentIdentifierMapping.Any(m => m.navigations.Count == path.Count && m.navigations.Zip(path, (o, i) => o.Name == i).All(r => r)))
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

                    var transparentIdentifierParameterName = /*resultSelectorOuterParameterName + */resultSelectorInnerParameterName;
                    var transparentIdentifierParameter = Expression.Parameter(resultSelector.ReturnType, transparentIdentifierParameterName);
                    parameterExpression = transparentIdentifierParameter;
                }


                // TODO: do we need to add the empty entry to ALL source mappings or just the one that is being processed?!
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

                        // in case of GroupJoin (optional navigation) source is hidden deeps since we also project the grouping
                        // we could remove the grouping in the future, but for nowe we need the grouping to properly recognize the LOJ pattern
                        if (navigationTree.Optional)
                        {
                            transparentIdentifierMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                        }
                    }
                }

                //foreach (var transparentIdentifierMappingElement in sourceMapping.TransparentIdentifierMapping)
                //{
                //    transparentIdentifierMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));

                //    // in case of GroupJoin (optional navigation) source is hidden deeps since we also project the grouping
                //    // we could remove the grouping in the future, but for nowe we need the grouping to properly recognize the LOJ pattern
                //    if (navigationTree.Optional)
                //    {
                //        transparentIdentifierMappingElement.path.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                //    }
                //}

                navigationPath.Add(navigation);

                foreach (var aSourceMapping in allSourceMappings)
                {
                    aSourceMapping.TransparentIdentifierMapping.Add((path: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }, navigations: navigationPath.ToList()));
                }

                //sourceMapping.TransparentIdentifierMapping.Add((path: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }, navigations: navigationPath.ToList()));

                if (pendingSelector != null)
                {
                    var psuev = new PendingSelectorUpdatingExpressionVisitor(oldParameterExpression, parameterExpression, navigationTree.Optional);
                    pendingSelector = (LambdaExpression)psuev.Visit(pendingSelector);
                }
            }
            else
            {
                navigationPath.Add(navigationTree.Navigation);
            }

            var result = (source: sourceExpression, parameter: parameterExpression, pendingSelector);
            foreach (var child in navigationTree.Children)
            {
                result = AddNavigationJoin2(
                    result.source,
                    result.parameter,
                    sourceMapping,
                    allSourceMappings,
                    child,
                    navigationPath.ToList(),
                    result.pendingSelector);
            }

            return result;
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
    }

    public class NavigationTreeNode
    {
        public INavigation Navigation { get; set; }
        public bool Optional { get; set; }
        public NavigationTreeNode Parent { get; set; }
        public List<NavigationTreeNode> Children { get; set; }

        public List<string> GeneratePath()
        {
            if (Parent == null)
            {
                return new List<string> { Navigation.Name };
            }
            else
            {
                var result = Parent.GeneratePath();
                result.Add(Navigation.Name);

                return result;
            }
        }

        public static NavigationTreeNode Create(IEnumerable<INavigation> expansionPath, bool optional)
        {
            if (expansionPath.Count() == 0)
            {
                return null;
            }

            var navigation = expansionPath.First();
            optional = optional || !navigation.ForeignKey.IsRequired || !navigation.IsDependentToPrincipal();
            var result = new NavigationTreeNode
            {
                Navigation = navigation,
                Optional = optional,
                Children = new List<NavigationTreeNode>(),
            };

            var child = Create(expansionPath.Skip(1), optional);
            if (child != null)
            {
                result.Children.Add(child);
                child.Parent = result;
            }

            return result;
        }

        public bool Contains(NavigationTreeNode other)
        {
            if (other.Navigation != Navigation)
            {
                return false;
            }

            return other.Children.All(oc => Children.Any(c => c.Contains(oc)));
        }

        public bool TryCombine(NavigationTreeNode other)
        {
            if (other.Navigation != Navigation)
            {
                return false;
            }

            foreach (var otherChild in other.Children)
            {
                var success = false;
                foreach (var child in Children)
                {
                    if (!success)
                    {
                        success = child.TryCombine(otherChild);
                    }
                }

                if (!success)
                {
                    Children.Add(otherChild);
                    otherChild.Parent = this;
                }
            }

            return true;
        }
    }

    public class NavigationFindingExpressionVisitor2 : ExpressionVisitor
    {
        private ParameterExpression _sourceParameter;

        public NavigationFindingExpressionVisitor2(
            ParameterExpression sourceParameter)
        {
            _sourceParameter = sourceParameter;
        }

        // TODO: clean up all this!!!!!!
        private List<string> GenerateInitialPath(Expression expression)
        {
            if (expression is ParameterExpression)
            {
                return new List<string>();
            }

            if (expression is MemberExpression memberExpression)
            {
                var innerPath = GenerateInitialPath(memberExpression.Expression);
                innerPath.Add(memberExpression.Member.Name);

                return innerPath;
            }

            // this probably should not be null, things will crash
            return null;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
            { 
                if (navigationBindingExpression.RootParameter == _sourceParameter
                    && navigationBindingExpression.Navigations.Count > 0)
                {
                    var inheritanceRoot = navigationBindingExpression.Navigations[0].ClrType != navigationBindingExpression.RootParameter.Type
                        && navigationBindingExpression.Navigations[0].DeclaringEntityType.GetAllBaseTypes().Any(t => t.ClrType == navigationBindingExpression.RootParameter.Type);

                    var navigationPath = NavigationTreeNode.Create(navigationBindingExpression.Navigations, inheritanceRoot);
                    if (!navigationBindingExpression.SourceMapping.FoundNavigations.Any(p => p.Contains(navigationPath)))
                    {
                        var success = false;
                        foreach (var foundNavigationPath in navigationBindingExpression.SourceMapping.FoundNavigations)
                        {
                            if (!success)
                            {
                                success = foundNavigationPath.TryCombine(navigationPath);
                            }
                        }

                        if (!success)
                        {
                            navigationBindingExpression.SourceMapping.FoundNavigations.Add(navigationPath);
                        }
                    }
                }

                return extensionExpression;
            }

            return base.VisitExtension(extensionExpression);
        }
    }

    public class NavigationReplacingExpressionVisitor2 : ExpressionVisitor
    {
        private ParameterExpression _previousParameter;
        private ParameterExpression _newParameter;

        public NavigationReplacingExpressionVisitor2(
            ParameterExpression previousParameter,
            ParameterExpression newParameter)
        {
            _previousParameter = previousParameter;
            _newParameter = newParameter;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _previousParameter)
                {
                    var transparentIdentifierAccessorPath = navigationBindingExpression.SourceMapping.TransparentIdentifierMapping.Where(
                        m => m.navigations.Count == navigationBindingExpression.Navigations.Count
                        && m.navigations.Zip(navigationBindingExpression.Navigations, (o, i) => o == i).All(e => e)).SingleOrDefault().path;

                    if (transparentIdentifierAccessorPath != null)
                    {
                        var result = BuildTransparentIdentifierAccessorExpression(_newParameter, navigationBindingExpression.SourceMapping.InitialPath, transparentIdentifierAccessorPath);

                        return result;
                    }
                }

                return navigationBindingExpression;
            }

            return base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newParameters = new List<ParameterExpression>();
            var parameterChanged = false;

            foreach (var parameter in lambdaExpression.Parameters)
            {
                if (parameter == _previousParameter)
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
    }
}
