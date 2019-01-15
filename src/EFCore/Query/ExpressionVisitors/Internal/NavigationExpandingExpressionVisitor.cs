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

        public static LambdaExpression CombineLambdas(LambdaExpression first, LambdaExpression second)
        {
            if (first == null)
            {
                return second;
            }

            if (second == null)
            {
                return first;
            }

            var lcev = new LambdaCombiningExpressionVisitor(first, first.Parameters[0], second.Parameters[0]);

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
        private List<(List<INavigation> from, List<string> to)> _transparentIdentifierAccessorMapping;
        private List<(List<string> path, IEntityType entityType)> _entityTypeAccessorMapping;

        public NavigationPropertyBindingExpressionVisitor2(
            ParameterExpression rootParameter,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
            List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping)
        {
            _rootParameter = rootParameter;
            _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
            _entityTypeAccessorMapping = entityTypeAccessorMapping;
        }

        private (ParameterExpression rootParameter, List<INavigation> navigations) TryFindMatchingTransparentIdentifierAccess(
            Expression expression,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMappingCandidates)
        {
            if (expression is ParameterExpression parameterExpression
                && (parameterExpression == _rootParameter || _rootParameter == null))
            {
                var navigations = transparentIdentifierAccessorMappingCandidates.Where(m => m.to.Count == 0).SingleOrDefault().from;

                return navigations != null
                    ? (rootParameter: _rootParameter, navigations: navigations.ToList())
                    : (rootParameter: null, navigations: null);
            }

            if (expression is MemberExpression memberExpression)
            {
                var matchingCandidates = transparentIdentifierAccessorMappingCandidates.Where(m => m.to.Any() && m.to.Last() == memberExpression.Member.Name);
                var newCandidates = matchingCandidates.Select(nc => (from: nc.from, to: nc.to.Take(nc.to.Count - 1).ToList())).ToList();
                if (newCandidates == null
                    || newCandidates.Any())
                {
                    var result = TryFindMatchingTransparentIdentifierAccess(memberExpression.Expression, newCandidates);

                    return result;
                }
            }

            return (null, new List<INavigation>());
        }

        private (ParameterExpression rootParameter, IEntityType entityType) TryFindMatchingEntityTypeAcess(
            Expression expression,
            List<(List<string> path, IEntityType entityType)> entityTypeAccessorMappingCandidates)
        {
            if (expression is ParameterExpression parameterExpression
                && (parameterExpression == _rootParameter || _rootParameter == null))
            {
                var entityType = entityTypeAccessorMappingCandidates.Where(m => m.path.Count == 0).SingleOrDefault().entityType;

                return entityType != null
                    ? (rootParameter: _rootParameter, entityType)
                    : (rootParameter: null, entityType: null);
            }

            if (expression is MemberExpression memberExpression)
            {
                var matchingCandidates = entityTypeAccessorMappingCandidates.Where(m => m.path.Any() && m.path.Last() == memberExpression.Member.Name);
                var newCandidates = matchingCandidates.Select(nc => (path: nc.path.Take(nc.path.Count - 1).ToList(), nc.entityType)).ToList();
                if (newCandidates == null
                    || newCandidates.Any())
                {
                    var result = TryFindMatchingEntityTypeAcess(memberExpression.Expression, newCandidates);

                    return result;
                }
            }

            return (null, null);
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var newExpression = Visit(memberExpression.Expression);

            if (newExpression is NavigationBindingExpression2 navigationBindingExpression)
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
                        navigation.GetTargetType());
                }
            }

            var tiaResult = TryFindMatchingTransparentIdentifierAccess(memberExpression, _transparentIdentifierAccessorMapping);
            if (tiaResult.rootParameter != null)
            {
                return new NavigationBindingExpression2(
                    memberExpression,
                    tiaResult.rootParameter,
                    tiaResult.navigations,
                    tiaResult.navigations.Last().GetTargetType());
            }

            var etaResult = TryFindMatchingEntityTypeAcess(memberExpression, _entityTypeAccessorMapping);
            if (etaResult.entityType != null)
            {
                return new NavigationBindingExpression2(
                    memberExpression,
                    etaResult.rootParameter,
                    new List<INavigation>(),
                    etaResult.entityType);
            }

            // TODO: try match custom projections based on entity type accessor mapping



            //var newExpression = memberExpression.Expression;
            //var entityType = Foo(memberExpression.Expression, _entityTypeAccessorMapping);
            //var rootExpression = entityType != null
            //    ? memberExpression.Expression
            //    : null;

            //var navigations = new List<INavigation>();

            //if (rootExpression == null)
            //{
            //    var result = TryFindMatchingTransparentIdentifierAccess(memberExpression.Expression, _transparentIdentifierAccessorMapping);
            //    rootExpression = result.rootExpression;
            //    navigations = result.navigations;

            //    if (navigations.Any())
            //    {
            //        // TODO: DRY this, this is used in several places!
            //        var navigation = navigations.Last();
            //        entityType = navigation.IsDependentToPrincipal()
            //            ? navigation.ForeignKey.PrincipalEntityType
            //            : navigation.ForeignKey.DeclaringEntityType;
            //    }
            //}

            //if (rootExpression == null)
            //{
            //    newExpression = Visit(memberExpression.Expression);
            //    if (newExpression is NavigationBindingExpression navigationBindingExpression)
            //    {
            //        rootExpression = navigationBindingExpression.Root;

            //        // TODO: DRY this, this is used in several places!
            //        var lastNavigation = navigationBindingExpression.Navigations.Last();
            //        entityType = lastNavigation.IsDependentToPrincipal()
            //            ? lastNavigation.ForeignKey.PrincipalEntityType
            //            : lastNavigation.ForeignKey.DeclaringEntityType;

            //        navigations.AddRange(navigationBindingExpression.Navigations);
            //    }
            //}

            //if (entityType != null)
            //{
            //    var navigation = entityType.FindNavigation(memberExpression.Member.Name);
            //    if (navigation != null)
            //    {
            //        navigations.Add(navigation);

            //        return new NavigationBindingExpression(rootExpression, navigations, memberExpression.Type);
            //    }
            //}

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
                var entityType = _entityTypeAccessorMapping.Where(m => m.path.Count == 0).Select(m => m.entityType).SingleOrDefault();
                if (entityType != null
                    && entityType.ClrType == parameterExpression.Type)
                {
                    return new NavigationBindingExpression2(
                        parameterExpression,
                        parameterExpression,
                        new List<INavigation>(),
                        entityType);
                }
            }

            return base.VisitParameter(parameterExpression);
        }
    }

    public class NavigationBindingExpression2 : Expression
    {
        //        private Type _type;

        public Expression Operand { get; }
        public ParameterExpression RootParameter { get; }
        public IEntityType EntityType { get; }
        public IReadOnlyList<INavigation> Navigations { get; }

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override bool CanReduce => true;
        public override Type Type => Operand.Type;//  _type;

        public override Expression Reduce()
            => Operand;

        public NavigationBindingExpression2(Expression operand, ParameterExpression rootParameter, List<INavigation> navigations, IEntityType entityType)
        {
            Operand = operand;
            RootParameter = rootParameter;
            Navigations = navigations.AsReadOnly();
            EntityType = entityType;
//            _type = operand.Type;
        }
    }

    public class NavigationPropertyBinder
    {
        public static (Expression root, IReadOnlyList<INavigation> navigations) BindNavigationProperties(
            Expression expression,
            ParameterExpression rootParameter,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierMapping,
            List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping)
        {
            var visitor = new NavigationPropertyBindingExpressionVisitor(rootParameter, transparentIdentifierMapping, entityTypeAccessorMapping);
            var result = visitor.Visit(expression);

            return result is NavigationBindingExpression navigationBindingExpression
                ? (source: navigationBindingExpression.Root, navigations: navigationBindingExpression.Navigations)
                : (source: expression, navigations: new List<INavigation>().AsReadOnly());
        }

        private class NavigationPropertyBindingExpressionVisitor : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<(List<INavigation> from, List<string> to)> _transparentIdentifierAccessorMapping;
            private List<(List<string> path, IEntityType entityType)> _entityTypeAccessorMapping;

            public NavigationPropertyBindingExpressionVisitor(
                ParameterExpression rootParameter,
                List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
                List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping)
            {
                _rootParameter = rootParameter;
                _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
                _entityTypeAccessorMapping = entityTypeAccessorMapping;
            }

            public (Expression root, IReadOnlyList<INavigation> navigations) BindNavigationProperties(Expression expression)
            {
                var result = Visit(expression);

                return result is NavigationBindingExpression navigationBindingExpression
                    ? (source: navigationBindingExpression.Root, navigations: navigationBindingExpression.Navigations)
                    : (source: expression, navigations: new List<INavigation>().AsReadOnly());
            }

            private IEntityType Foo(Expression expression, List<(List<string> path, IEntityType entityType)> entityTypeAccessorMappingCandidates)
            {
                if (expression is ParameterExpression parameterExpression
                    && (parameterExpression == _rootParameter || _rootParameter == null))
                {
                    var entityType = entityTypeAccessorMappingCandidates.Where(m => m.path.Count == 0).SingleOrDefault().entityType;

                    return entityType;
                }

                if (expression is MemberExpression memberExpression)
                {
                    var matchingCandidates = entityTypeAccessorMappingCandidates.Where(m => m.path.Any() && m.path.Last() == memberExpression.Member.Name);
                    var newCandidates = matchingCandidates.Select(nc => (path: nc.path.Take(nc.path.Count - 1).ToList(), nc.entityType)).ToList();
                    if (newCandidates == null
                        || newCandidates.Any())
                    {
                        var entityType = Foo(memberExpression.Expression, newCandidates);

                        return entityType;
                    }
                }

                return null;
            }

            // TODO: need to combine both of those methods, in case there is entity type mapping AND transparent identifier mapping
            // or maybe that never happens????

            private (Expression rootExpression, List<INavigation> navigations) TryFindMatchingTransparentIdentifierAccess(
                Expression expression,
                List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMappingCandidates)
            {
                if (expression is ParameterExpression parameterExpression
                    && (parameterExpression == _rootParameter || _rootParameter == null))
                {
                    var navigations = transparentIdentifierAccessorMappingCandidates.Where(m => m.to.Count == 0).SingleOrDefault().from;
                    var rootExpression = _rootParameter;

                    return (rootExpression, navigations.ToList());
                }

                if (expression is MemberExpression memberExpression)
                {
                    var matchingCandidates = transparentIdentifierAccessorMappingCandidates.Where(m => m.to.Any() && m.to.Last() == memberExpression.Member.Name);
                    var newCandidates = matchingCandidates.Select(nc => (from: nc.from, to: nc.to.Take(nc.to.Count -1).ToList())).ToList();
                    if (newCandidates == null
                        || newCandidates.Any())
                    {
                        var result = TryFindMatchingTransparentIdentifierAccess(memberExpression.Expression, newCandidates);

                        return result;
                    }
                }

                return (null, new List<INavigation>());
            }

            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                var newExpression = memberExpression.Expression;
                var entityType = Foo(memberExpression.Expression, _entityTypeAccessorMapping);
                var rootExpression = entityType != null
                    ? memberExpression.Expression
                    : null;

                var navigations = new List<INavigation>();

                if (rootExpression == null)
                {
                    var result = TryFindMatchingTransparentIdentifierAccess(memberExpression.Expression, _transparentIdentifierAccessorMapping);
                    rootExpression = result.rootExpression;
                    navigations = result.navigations;

                    if (navigations.Any())
                    {
                        // TODO: DRY this, this is used in several places!
                        var navigation = navigations.Last();
                        entityType = navigation.IsDependentToPrincipal()
                            ? navigation.ForeignKey.PrincipalEntityType
                            : navigation.ForeignKey.DeclaringEntityType;
                    }
                }

                if (rootExpression == null)
                {
                    newExpression = Visit(memberExpression.Expression);
                    if (newExpression is NavigationBindingExpression navigationBindingExpression)
                    {
                        rootExpression = navigationBindingExpression.Root;

                        // TODO: DRY this, this is used in several places!
                        var lastNavigation = navigationBindingExpression.Navigations.Last();
                        entityType = lastNavigation.IsDependentToPrincipal()
                            ? lastNavigation.ForeignKey.PrincipalEntityType
                            : lastNavigation.ForeignKey.DeclaringEntityType;

                        navigations.AddRange(navigationBindingExpression.Navigations);
                    }
                }

                if (entityType != null)
                {
                    var navigation = entityType.FindNavigation(memberExpression.Member.Name);
                    if (navigation != null)
                    {
                        navigations.Add(navigation);

                        return new NavigationBindingExpression(rootExpression, navigations, memberExpression.Type);
                    }
                }

                return memberExpression;
            }
        }

        private class NavigationBindingExpression : Expression
        {
            private Type _type;

            public Expression Root { get; }
            public IReadOnlyList<INavigation> Navigations { get; }

            public override ExpressionType NodeType => ExpressionType.Extension;
            public override Type Type => _type;

            public NavigationBindingExpression(Expression root, List<INavigation> navigations, Type type)
            {
                Root = root;
                Navigations = navigations.AsReadOnly();
                _type = type;
            }
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

    //public class PendingSelectorsApplyingExpressionVisitor : LinqQueryExpressionVisitorBase
    //{
    //    private IModel _model;
    //    public PendingSelectorsApplyingExpressionVisitor(IModel model)
    //    {
    //        _model = model;
    //    }

    //    protected override Expression VisitExtension(Expression extensionExpression)
    //    {
    //        if (extensionExpression is NavigationExpansionExpression nee
    //            && nee.PendingSelector != null)
    //        {
    //            // TODO: fix this!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    //            return null;






    //            //var topPendingSelectorParameter = nee.PendingSelector.Parameters[0];
    //            //var selector = Expression.Lambda(nee.CurrentParameter, nee.CurrentParameter);


    //            //var lcev = new LambdaCombiningExpressionVisitor(nee.PendingSelector, nee.CurrentParameter);
    //            //var combinedSelector = lcev.Visit(selector);

    //            ////var pscev = new PreviousSelectorCompensatingExpressionVisitor(nee.PendingSelector, nee.CurrentParameter);
    //            ////var compensatedSelector = pscev.Visit(selector);

    //            ////var nrev = new NavigationReplacingExpressionVisitor(
    //            ////    _model,
    //            ////    nee.CurrentParameter,
    //            ////    nee.FirstSelectorParameter ?? nee.CurrentParameter, // TODO: is this correct????
    //            ////    nee.CurrentParameter,
    //            ////    nee.TransparentIdentifierAccessorMapping,
    //            ////    nee.EntityTypeAccessorMapping);

    //            //var binder = new NavigationPropertyBindingExpressionVisitor2(
    //            //    nee.CurrentParameter,
    //            //    nee.TransparentIdentifierAccessorMapping,
    //            //    nee.EntityTypeAccessorMapping);

    //            //var boundSelector = binder.Visit(combinedSelector);

    //            //var nrev = new NavigationReplacingExpressionVisitor2(
    //            //    nee.CurrentParameter,
    //            //    nee.CurrentParameter,
    //            //    nee.TransparentIdentifierAccessorMapping);

    //            //var newSelector = nrev.Visit(boundSelector);

    //            //var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(nee.CurrentParameter.Type, ((LambdaExpression)newSelector).Body.Type);
    //            //var result = Expression.Call(selectorMethodInfo, nee.Operand, newSelector);

    //            //return result;
    //        }

    //        return base.VisitExtension(extensionExpression);
    //    }
    //}

    public class NavigationExpandingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        private IModel _model;

        public NavigationExpandingExpressionVisitor(IModel model)
        {
            _model = model;
        }

        private class EntityTypeAccessorMappingGenerator : ExpressionVisitor
        {
            private ParameterExpression _rootParameter;
            private List<(List<INavigation> from, List<string> to)> _transparentIdentifierMapping;
            private List<(List<string> path, IEntityType entityType)> _originalEntityAccessorMapping;
            private List<string> _currentPath;

            public List<(List<string> path, IEntityType entityType)> NewEntityAccessorMapping { get; }

            public EntityTypeAccessorMappingGenerator(
                ParameterExpression rootParameter,
                List<(List<INavigation> from, List<string> to)> transparentIdentifierMapping,
                List<(List<string> path, IEntityType entityType)> entityAccessorMapping)
            {
                _rootParameter = rootParameter;
                _transparentIdentifierMapping = transparentIdentifierMapping;
                _originalEntityAccessorMapping = entityAccessorMapping;
                _currentPath = new List<string>();

                NewEntityAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            }

            // prune these nodes, we only want to look for entities accessible in the result
            protected override Expression VisitMember(MemberExpression memberExpression)
                => memberExpression;

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
                => methodCallExpression;

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
                        NewEntityAccessorMapping.Add((path: _currentPath.ToList(), entityType: navigationBindingExpression.EntityType));
                    }

                    return navigationBindingExpression;
                }

                return base.VisitExtension(extensionExpression);
            }
        }

        private class PreviousSelectorCompensatingExpressionVisitor : ExpressionVisitor
        {
            private List<LambdaExpression> _pendingSelectors;
            private ParameterExpression _parameterExpression;

            public PreviousSelectorCompensatingExpressionVisitor(List<LambdaExpression> pendingSelectors, ParameterExpression parameterExpression)
            {
                _pendingSelectors = pendingSelectors;
                _parameterExpression = parameterExpression;
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
                if (parameterExpression == _parameterExpression && _pendingSelectors.Any())
                {
                    var parameterToReplace = _parameterExpression;
                    var result = (Expression)parameterExpression;
                    for (var i = _pendingSelectors.Count - 1; i >= 0; i--)
                    {
                        var replaceWith = _pendingSelectors[i].Body;
                        var prev = new ParameterReplacingExpressionVisitor(parameterToReplace, replaceWith);
                        result = prev.Visit(result);
                        parameterToReplace = _pendingSelectors[i].Parameters[0]; // TODO: can there be more parameters e.g. in join result selector?
                    }

                    return result;
                }

                return base.VisitParameter(parameterExpression);
            }

            private class ParameterReplacingExpressionVisitor: ExpressionVisitor
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

            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                // TODO: this method visits too much! need to optimize!
                var newSource = Visit(memberExpression.Expression);
                if (newSource is NewExpression newExpression)
                {
                    var matchingMemberIndex = newExpression.Members.Select((m, i) => new { index = i, match = m == memberExpression.Member }).Where(r => r.match).SingleOrDefault()?.index;
                    if (matchingMemberIndex.HasValue)
                    {
                        return Visit(newExpression.Arguments[matchingMemberIndex.Value]);
                    }
                }

                return newSource != memberExpression.Expression
                    ? memberExpression.Update(newSource)
                    : memberExpression;
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
            {
                return base.VisitMethodCall(methodCallExpression);
            }
        }

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

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableGroupJoinMethodInfo))
            {
                var result = ProcessGroupJoin(methodCallExpression);

                return result;
            }

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
            var predicate = methodCallExpression.Arguments[1];
            var currentParameter = predicate.UnwrapQuote().Parameters[0];
            var transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var entityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var appliedSelector = default(LambdaExpression);
            var pendingSelector = default(LambdaExpression);
            var foundNavigations = new List<NavigationTreeNode>();
            var finalProjectionPath = new List<string>();

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                currentParameter = navigationExpansionExpression.CurrentParameter ?? currentParameter;
                transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                entityTypeAccessorMapping = navigationExpansionExpression.EntityTypeAccessorMapping;

                appliedSelector = navigationExpansionExpression.AppliedSelector;
                pendingSelector = navigationExpansionExpression.PendingSelector;
                foundNavigations = navigationExpansionExpression.FoundNavigations;
                finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;
            }

            var combinedSelector = pendingSelector;
            var compensatedPredicate = ExpressionExtensions.CombineLambdas(combinedSelector, predicate.UnwrapQuote());

            compensatedPredicate = (LambdaExpression)Visit(compensatedPredicate);

            var binder = new NavigationPropertyBindingExpressionVisitor2(
                currentParameter,
                transparentIdentifierAccessorMapping,
                entityTypeAccessorMapping);

            var boundPredicate = binder.Visit(compensatedPredicate);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(currentParameter);
            boundPredicate = (LambdaExpression)cnrev.Visit(boundPredicate);

            var nfev = new NavigationFindingExpressionVisitor2(currentParameter, foundNavigations);
            nfev.Visit(boundPredicate);

            var result = (source, parameter: currentParameter, pendingSelector);
            if (nfev.FoundNavigations.Any())
            {
                foreach (var navigationTree in nfev.FoundNavigations)
                {
                    result = AddNavigationJoin(
                        result.source,
                        result.parameter,
                        navigationTree,
                        new List<INavigation>(),
                        finalProjectionPath,
                        transparentIdentifierAccessorMapping,
                        pendingSelector);
                }
            }

            var previousParameter = currentParameter;
            currentParameter = result.parameter;
            pendingSelector = result.pendingSelector;

            var nrev = new NavigationReplacingExpressionVisitor2(
                previousParameter,
                currentParameter,
                transparentIdentifierAccessorMapping);

            var newPredicate = nrev.Visit(boundPredicate);

            var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.parameter.Type);
            var rewritten = Expression.Call(newMethodInfo, result.source, newPredicate);

            return new NavigationExpansionExpression(
                rewritten,
                result.parameter,
                transparentIdentifierAccessorMapping,
                entityTypeAccessorMapping,
                appliedSelector,
                pendingSelector,
                foundNavigations,
                finalProjectionPath,
                methodCallExpression.Type);
         }

        private Expression ProcessSelect(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var selector = methodCallExpression.Arguments[1];
            var currentParameter = selector.UnwrapQuote().Parameters[0];
            var transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var entityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var appliedSelector = default(LambdaExpression);
            var pendingSelector = default(LambdaExpression);
            var foundNavigations = new List<NavigationTreeNode>();
            var finalProjectionPath = new List<string>();

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                currentParameter = navigationExpansionExpression.CurrentParameter ?? currentParameter;
                transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                entityTypeAccessorMapping = navigationExpansionExpression.EntityTypeAccessorMapping;
                appliedSelector = navigationExpansionExpression.AppliedSelector;
                pendingSelector = navigationExpansionExpression.PendingSelector ?? Expression.Lambda(currentParameter, currentParameter);
                foundNavigations = navigationExpansionExpression.FoundNavigations;
                finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;
            }

            var combinedSelector = pendingSelector;// ExpressionExtensions.CombineLambdas(appliedSelector, pendingSelector);
            var compensatedSelector = ExpressionExtensions.CombineLambdas(combinedSelector, selector.UnwrapQuote());

            compensatedSelector = (LambdaExpression)Visit(compensatedSelector);

            var binder = new NavigationPropertyBindingExpressionVisitor2(currentParameter, transparentIdentifierAccessorMapping, entityTypeAccessorMapping);
            var boundSelector = (LambdaExpression)binder.Visit(compensatedSelector);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(currentParameter);
            boundSelector = (LambdaExpression)cnrev.Visit(boundSelector);

            var nfev = new NavigationFindingExpressionVisitor2(currentParameter, foundNavigations);
            nfev.Visit(boundSelector);

            var result = (source, parameter: currentParameter, pendingSelector);
            if (nfev.FoundNavigations.Any())
            {
                foreach (var navigationPath in nfev.FoundNavigations)
                {
                    result = AddNavigationJoin(
                        result.source,
                        result.parameter,
                        navigationPath,
                        new List<INavigation>(),
                        finalProjectionPath,
                        transparentIdentifierAccessorMapping,
                        pendingSelector);
                }
            }

            var previousParameter = currentParameter;
            currentParameter = result.parameter;
            pendingSelector = result.pendingSelector;

            var nrev = new NavigationReplacingExpressionVisitor2(
                previousParameter,
                currentParameter,
                transparentIdentifierAccessorMapping);

            var newSelector = nrev.Visit(boundSelector);
            pendingSelector = (LambdaExpression)newSelector;

            return new NavigationExpansionExpression(
                result.source,
                result.parameter,
                transparentIdentifierAccessorMapping,
                entityTypeAccessorMapping,
                appliedSelector,
                pendingSelector,
                foundNavigations,
                finalProjectionPath,
                methodCallExpression.Type);
        }

        private Expression ProcessOrderBy(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var keySelector = methodCallExpression.Arguments[1];
            var currentParameter = keySelector.UnwrapQuote().Parameters[0];
            var transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var entityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var appliedSelector = default(LambdaExpression);
            var pendingSelector = default(LambdaExpression);
            var foundNavigations = new List<NavigationTreeNode>();
            var finalProjectionPath = new List<string>();

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                currentParameter = navigationExpansionExpression.CurrentParameter ?? currentParameter;
                transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                entityTypeAccessorMapping = navigationExpansionExpression.EntityTypeAccessorMapping;
                appliedSelector = navigationExpansionExpression.AppliedSelector;
                pendingSelector = navigationExpansionExpression.PendingSelector;
                foundNavigations = navigationExpansionExpression.FoundNavigations;
                finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;
            }

            var combinedSelector = pendingSelector;// ExpressionExtensions.CombineLambdas(appliedSelector, pendingSelector);
            var compensatedKeySelector = ExpressionExtensions.CombineLambdas(combinedSelector, keySelector.UnwrapQuote());

            compensatedKeySelector = (LambdaExpression)Visit(compensatedKeySelector);

            var binder = new NavigationPropertyBindingExpressionVisitor2(
                currentParameter,
                transparentIdentifierAccessorMapping,
                entityTypeAccessorMapping);

            var boundKeySelector = binder.Visit(compensatedKeySelector);

            var cnrev = new CollectionNavigationRewritingExpressionVisitor2(currentParameter);
            boundKeySelector = (LambdaExpression)cnrev.Visit(boundKeySelector);

            var nfev = new NavigationFindingExpressionVisitor2(currentParameter, foundNavigations);
            nfev.Visit(boundKeySelector);

            var result = (source, parameter: currentParameter, pendingSelector);
            if (nfev.FoundNavigations.Any())
            {
                foreach (var navigationTree in nfev.FoundNavigations)
                {
                    result = AddNavigationJoin(
                        result.source,
                        result.parameter,
                        navigationTree,
                        new List<INavigation>(),
                        finalProjectionPath,
                        transparentIdentifierAccessorMapping,
                        pendingSelector);
                }
            }

            var previousParameter = currentParameter;
            currentParameter = result.parameter;
            pendingSelector = result.pendingSelector;

            var nrev = new NavigationReplacingExpressionVisitor2(
                previousParameter,
                currentParameter,
                transparentIdentifierAccessorMapping);

            var newKeySelector = nrev.Visit(boundKeySelector);

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(result.parameter.Type, newKeySelector.UnwrapQuote().Body.Type);
            var rewritten = Expression.Call(newMethodInfo, result.source, newKeySelector);

            return new NavigationExpansionExpression(
                rewritten,
                result.parameter,
                transparentIdentifierAccessorMapping,
                entityTypeAccessorMapping,
                appliedSelector,
                pendingSelector,
                foundNavigations,
                finalProjectionPath,
                methodCallExpression.Type);
        }

        private Expression ProcessSelectManyWithResultOperator(MethodCallExpression methodCallExpression)
        {
            var outerSource = Visit(methodCallExpression.Arguments[0]);
            var innerSource = Visit(methodCallExpression.Arguments[1]);

            var resultSelector = methodCallExpression.Arguments[2];
            var outerCurrentParameter = resultSelector.UnwrapQuote().Parameters[0];
            var innerCurrentParameter = resultSelector.UnwrapQuote().Parameters[1];
            var outerFirstSelectorParameter = outerCurrentParameter;
            var innerFirstSelectorParameter = innerCurrentParameter;
            var outerOriginalParameter = outerCurrentParameter;
            var innerOriginalrParameter = innerCurrentParameter;

            var outerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var outerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var outerAppliedSelector = default(LambdaExpression);
            var outerPendingSelector = default(LambdaExpression);
            var outerFoundNavigations = new List<NavigationTreeNode>();
            var outerFinalProjectionPath = new List<string>();

            var innerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var innerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var innerAppliedSelector = default(LambdaExpression);
            var innerPendingSelector = default(LambdaExpression);
            var innerFoundNavigations = new List<NavigationTreeNode>();
            var innerFinalProjectionPath = new List<string>();

            // TODO: unwrap lambda body here
            if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            {
                outerSource = outerNavigationExpansionExpression.Operand;
                outerTransparentIdentifierAccessorMapping = outerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
                outerEntityTypeAccessorMapping = outerNavigationExpansionExpression.EntityTypeAccessorMapping;
                outerAppliedSelector = outerNavigationExpansionExpression.AppliedSelector;
                outerPendingSelector = outerNavigationExpansionExpression.PendingSelector;
                outerFoundNavigations = outerNavigationExpansionExpression.FoundNavigations;
                outerFinalProjectionPath = outerNavigationExpansionExpression.FinalProjectionPath;
            }

            if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            {
                innerSource = innerNavigationExpansionExpression.Operand;
                innerTransparentIdentifierAccessorMapping = innerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
                innerEntityTypeAccessorMapping = innerNavigationExpansionExpression.EntityTypeAccessorMapping;
                innerAppliedSelector = innerNavigationExpansionExpression.AppliedSelector;
                innerPendingSelector = innerNavigationExpansionExpression.PendingSelector;
                innerFoundNavigations = innerNavigationExpansionExpression.FoundNavigations;
                innerFinalProjectionPath = innerNavigationExpansionExpression.FinalProjectionPath;
            }

            // TODO: finish this!!!

            return methodCallExpression;
        }

        private Expression ProcessJoin(MethodCallExpression methodCallExpression)
        {
            // TODO:  implement

            return methodCallExpression;
        }

        private Expression ProcessGroupJoin(MethodCallExpression methodCallExpression)
        {
            var outerSource = Visit(methodCallExpression.Arguments[0]);
            var innerSource = Visit(methodCallExpression.Arguments[1]);

            var outerKeySelector = methodCallExpression.Arguments[2];
            var innerKeySelector = methodCallExpression.Arguments[3];
            var resultSelector = methodCallExpression.Arguments[4];

            var outerKeySelectorCurrentParameter = outerKeySelector.UnwrapQuote().Parameters[0];
            var innerKeySelectorCurrentParameter = innerKeySelector.UnwrapQuote().Parameters[0];
            var firstResultSelectorCurrentParameter = resultSelector.UnwrapQuote().Parameters[0];
            var secondResultSelectorCurrentParameter = resultSelector.UnwrapQuote().Parameters[1];

            // TODO: shouldnt those be null?????
            var outerFirstSelectorParameter = outerKeySelectorCurrentParameter;
            var innerFirstSelectorParameter = innerKeySelectorCurrentParameter;

            var outerKeySelectorOriginalParameter = outerKeySelectorCurrentParameter;
            var innerKeySelectorOriginalParameter = innerKeySelectorCurrentParameter;
            var firstResultSelectorOriginalParameter = firstResultSelectorCurrentParameter;
            var secondResultSelectorOriginalParameter = secondResultSelectorCurrentParameter;

            var outerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var outerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var outerAppliedSelector = default(LambdaExpression);
            var outerPendingSelector = default(LambdaExpression);
            var outerFoundNavigations = new List<NavigationTreeNode>();
            var outerFinalProjectionPath = new List<string>();

            var innerTransparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var innerEntityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var innerAppliedSelector = default(LambdaExpression);
            var innerPendingSelector = default(LambdaExpression);
            var innerFoundNavigations = new List<NavigationTreeNode>();
            var innerFinalProjectionPath = new List<string>();

            if (outerSource is NavigationExpansionExpression outerNavigationExpansionExpression)
            {
                outerSource = outerNavigationExpansionExpression.Operand;

                //outerFirstSelectorParameter = outerNavigationExpansionExpression.FirstSelectorParameter ?? outerFirstSelectorParameter;
                //outerKeySelectorCurrentParameter = outerNavigationExpansionExpression.CurrentParameter ?? outerKeySelectorCurrentParameter;

                outerTransparentIdentifierAccessorMapping = outerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
                outerEntityTypeAccessorMapping = outerNavigationExpansionExpression.EntityTypeAccessorMapping;

                outerAppliedSelector = outerNavigationExpansionExpression.AppliedSelector;
                outerPendingSelector = outerNavigationExpansionExpression.PendingSelector;

                outerFoundNavigations = outerNavigationExpansionExpression.FoundNavigations;
                outerFinalProjectionPath = outerNavigationExpansionExpression.FinalProjectionPath;
            }

            if (innerSource is NavigationExpansionExpression innerNavigationExpansionExpression)
            {
                innerSource = innerNavigationExpansionExpression.Operand;

                //innerFirstSelectorParameter = innerNavigationExpansionExpression.FirstSelectorParameter ?? innerFirstSelectorParameter;
                //innerKeySelectorCurrentParameter = innerNavigationExpansionExpression.CurrentParameter ?? innerKeySelectorCurrentParameter;

                innerTransparentIdentifierAccessorMapping = innerNavigationExpansionExpression.TransparentIdentifierAccessorMapping;
                innerEntityTypeAccessorMapping = innerNavigationExpansionExpression.EntityTypeAccessorMapping;

                innerAppliedSelector = innerNavigationExpansionExpression.AppliedSelector;
                innerPendingSelector = innerNavigationExpansionExpression.PendingSelector;

                innerFoundNavigations = innerNavigationExpansionExpression.FoundNavigations;
                innerFinalProjectionPath = innerNavigationExpansionExpression.FinalProjectionPath;
            }

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

            var outerCombinedSelector = ExpressionExtensions.CombineLambdas(outerAppliedSelector, outerPendingSelector);
            var compensatedOuterKeySelector = ExpressionExtensions.CombineLambdas(outerCombinedSelector, outerKeySelector.UnwrapQuote());

            //var outerCnrev = new CollectionNavigationRewritingExpressionVisitor(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            //compensatedOuterKeySelector = (LambdaExpression)outerCnrev.Visit(compensatedOuterKeySelector);

            var outerBinder = new NavigationPropertyBindingExpressionVisitor2(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping);
            var boundOuterKeySelector = (LambdaExpression)outerBinder.Visit(compensatedOuterKeySelector);

            var outerCnrev = new CollectionNavigationRewritingExpressionVisitor2(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter);
            compensatedOuterKeySelector = (LambdaExpression)outerCnrev.Visit(boundOuterKeySelector);

            var outerNfev = new NavigationFindingExpressionVisitor2(outerKeySelectorCurrentParameter, outerFoundNavigations);
            outerNfev.Visit(compensatedOuterKeySelector);







            //var outerNfev = new NavigationFindingExpressionVisitor(outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            //outerNfev.Visit(compensatedOuterKeySelector);





            var innerCombinedSelector = ExpressionExtensions.CombineLambdas(innerAppliedSelector, innerPendingSelector);
            var compensatedInnerKeySelector = ExpressionExtensions.CombineLambdas(innerCombinedSelector, innerKeySelector.UnwrapQuote());

            var innerBinder = new NavigationPropertyBindingExpressionVisitor2(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping);
            var boundInnerKeySelector = innerBinder.Visit(compensatedInnerKeySelector);

            var innerCnrev = new CollectionNavigationRewritingExpressionVisitor2(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter);
            compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(boundInnerKeySelector);

            //var innerCnrev = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(compensatedInnerKeySelector);

            //var innerNfev = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //innerNfev.Visit(compensatedInnerKeySelector);

            var innerNfev = new NavigationFindingExpressionVisitor2(innerKeySelectorCurrentParameter, innerFoundNavigations);
            innerNfev.Visit(compensatedInnerKeySelector);





            var compensatedResultSelector = ExpressionExtensions.CombineLambdas(outerCombinedSelector, resultSelector.UnwrapQuote());
            compensatedResultSelector = ExpressionExtensions.CombineLambdas(innerCombinedSelector, compensatedResultSelector);




            //var resultCnrev1 = new CollectionNavigationRewritingExpressionVisitor(outerFirstSelectorParameter ?? firstResultSelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            //compensatedResultSelector = (LambdaExpression)resultCnrev1.Visit(compensatedResultSelector);

            //var resultCnrev2 = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //compensatedResultSelector = (LambdaExpression)resultCnrev2.Visit(compensatedResultSelector);



            //var innerNfev1 = new NavigationFindingExpressionVisitor(outerFirstSelectorParameter ?? firstResultSelectorOriginalParameter, outerTransparentIdentifierAccessorMapping, outerEntityTypeAccessorMapping, outerFoundNavigations);
            //innerNfev1.Visit(compensatedInnerKeySelector);

            //var innerNfev2 = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter, innerTransparentIdentifierAccessorMapping, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //innerNfev2.Visit(compensatedInnerKeySelector);

            var innerNfev1 = new NavigationFindingExpressionVisitor2(outerKeySelectorCurrentParameter, outerFoundNavigations);
            innerNfev1.Visit(compensatedInnerKeySelector);

            var innerNfev2 = new NavigationFindingExpressionVisitor2(innerKeySelectorCurrentParameter, innerFoundNavigations);
            innerNfev2.Visit(compensatedInnerKeySelector);





            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

            var outerResult = (source: outerSource, parameter: outerKeySelectorCurrentParameter, pendingSelector: outerPendingSelector);

            if (outerFoundNavigations.Any())
            //if (outerNfev.FoundNavigationPaths.Any())
            {
                foreach (var navigationPath in outerFoundNavigations)
                //foreach (var navigationPath in outerNfev.FoundNavigationPaths)
                {
                    outerResult = AddNavigationJoin(
                        outerResult.source,
                        outerResult.parameter,
                        navigationPath,
                        new List<INavigation>(),
                        outerFinalProjectionPath,
                        outerTransparentIdentifierAccessorMapping,
                        outerPendingSelector);
                }
            }

            var newOuterSource = outerResult.source;
            var outerKeySelectorPreviousParameter = outerKeySelectorCurrentParameter;
            outerKeySelectorCurrentParameter = outerResult.parameter;
            firstResultSelectorCurrentParameter = outerResult.parameter;
            outerPendingSelector = outerResult.pendingSelector;

            //// here we need 3 parameters:
            //// - original parameter: parameter in the original lambda. It will be present in the parameter list since pending selector compensation only modified the body.
            //// - root parameter: parameter representing root on the navigation chain. It was injected to the lambda after pending selector compensation.
            //// - current parameter: parameter representing current transparent identifier. All parameter expressions should be replaced to it
            //var outerNrev = new NavigationReplacingExpressionVisitor(
            //    _model,
            //    outerKeySelectorOriginalParameter,
            //    outerFirstSelectorParameter ?? outerKeySelectorOriginalParameter,//  rootParameter, <--- TODO: is this correct???????
            //    outerKeySelectorCurrentParameter,
            //    outerTransparentIdentifierAccessorMapping,
            //    outerEntityTypeAccessorMapping);

            var outerNrev = new NavigationReplacingExpressionVisitor2(
                outerKeySelectorPreviousParameter,
                outerKeySelectorCurrentParameter,
                outerTransparentIdentifierAccessorMapping);

            var newOuterKeySelector = outerNrev.Visit(compensatedOuterKeySelector);

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

            //var innerCombinedSelector = ExpressionExtensions.CombineLambdas(innerAppliedSelector, innerPendingSelector);
            //var compensatedInnerKeySelector = ExpressionExtensions.CombineLambdas(innerCombinedSelector, innerKeySelector.UnwrapQuote());

            //var innerCnrev = new CollectionNavigationRewritingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //compensatedInnerKeySelector = (LambdaExpression)innerCnrev.Visit(compensatedInnerKeySelector);

            //var innerNfev = new NavigationFindingExpressionVisitor(innerFirstSelectorParameter ?? innerKeySelectorOriginalParameter, innerEntityTypeAccessorMapping, innerFoundNavigations);
            //innerNfev.Visit(compensatedInnerKeySelector);

            var innerResult = (source: innerSource, parameter: innerKeySelectorCurrentParameter, pendingSelector: innerPendingSelector);
            if (innerFoundNavigations.Any())
            //if (innerNfev.FoundNavigationPaths.Any())
            {
                foreach (var navigationPath in innerFoundNavigations)
                //foreach (var navigationPath in innerNfev.FoundNavigationPaths)
                {
                    innerResult = AddNavigationJoin(
                        innerResult.source,
                        innerResult.parameter,
                        navigationPath,
                        new List<INavigation>(),
                        innerFinalProjectionPath,
                        innerTransparentIdentifierAccessorMapping,
                        innerPendingSelector);
                }
            }

            var newInnerSource = innerResult.source;
            var innerKeySelectorPreviousParameter = innerKeySelectorCurrentParameter;
            innerKeySelectorCurrentParameter = innerResult.parameter;
            innerPendingSelector = innerResult.pendingSelector;

            //// PROBABLY WRONG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            //secondResultSelectorCurrentParameter = innerResult.parameter;


            // here we need 3 parameters:
            // - original parameter: parameter in the original lambda. It will be present in the parameter list since pending selector compensation only modified the body.
            // - root parameter: parameter representing root on the navigation chain. It was injected to the lambda after pending selector compensation.
            // - current parameter: parameter representing current transparent identifier. All parameter expressions should be replaced to it
            var innerNrev = new NavigationReplacingExpressionVisitor2(
                innerKeySelectorPreviousParameter,
                innerKeySelectorCurrentParameter,
                innerTransparentIdentifierAccessorMapping);

            var newInnerKeySelector = innerNrev.Visit(compensatedInnerKeySelector);




            var firstResultNrev = new NavigationReplacingExpressionVisitor2(
                firstResultSelectorOriginalParameter,
                firstResultSelectorCurrentParameter,
                outerTransparentIdentifierAccessorMapping);

            var newResultSelector = firstResultNrev.Visit(compensatedResultSelector);


            //// TODO: fix the second argument - need to convert to collection (somehow)
            //var secondResultNrev = new NavigationReplacingExpressionVisitor(
            //    _model,
            //    secondResultSelectorOriginalParameter,
            //    innerFirstSelectorParameter ?? secondResultSelectorOriginalParameter,
            //    secondResultSelectorCurrentParameter,
            //    innerTransparentIdentifierAccessorMapping,
            //    innerEntityTypeAccessorMapping);

            //newResultSelector = secondResultNrev.Visit(newResultSelector);






            // TODO:
            //
            // WHAT DO WE DO ABOUT RESULT SELECTOR????? - does join/groupjoin need to be "terminating operation"??
            // or is there some smart way to delay the selector

            var newMethodInfo = methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(
                outerResult.parameter.Type,
                innerResult.parameter.Type,
                outerKeySelector.UnwrapQuote().Body.Type,
                resultSelector.UnwrapQuote().Body.Type);

            //var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.parameter.Type);
            var rewritten = Expression.Call(newMethodInfo, newOuterSource, newInnerSource, newOuterKeySelector, newInnerKeySelector, newResultSelector);

            // i guess we need to reset - no way to fit both sources here otherwise


            return rewritten;

            //return new NavigationExpansionExpression(
            //    rewritten,
            //    firstSelectorParameter,
            //    result.parameter,
            //    transparentIdentifierAccessorMapping,
            //    entityTypeAccessorMapping,
            //    appliedSelector,
            //    pendingSelector,
            //    foundNavigations,
            //    finalProjectionPath,
            //    methodCallExpression.Type);
        }

        private Expression ProcessTerminatingOperation(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            //var currentParameter = Expression.Parameter(source.Type.GetGenericArguments()[0], methodCallExpression.Method.Name.ToLower());
            var currentParameter = Expression.Parameter(source.Type.GetGenericArguments()[0], source.Type.GetGenericArguments()[0].GenerateParameterName());

            //var currentParameter = default(ParameterExpression);

            //var firstSelectorParameter = default(ParameterExpression);
            var transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
            var entityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
            var appliedSelector = default(LambdaExpression);
            var pendingSelector = default(LambdaExpression);
            var foundNavigations = new List<NavigationTreeNode>();
            var finalProjectionPath = new List<string>();

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                currentParameter = navigationExpansionExpression.CurrentParameter ?? currentParameter;
                //firstSelectorParameter = navigationExpansionExpression.FirstSelectorParameter;
                //currentParameter = navigationExpansionExpression.CurrentParameter ?? currentParameter;
                transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                entityTypeAccessorMapping = navigationExpansionExpression.EntityTypeAccessorMapping;

                appliedSelector = navigationExpansionExpression.AppliedSelector;
                pendingSelector = navigationExpansionExpression.PendingSelector;

                foundNavigations = navigationExpansionExpression.FoundNavigations;
                finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;

                if (pendingSelector != null)
                {
                    // TODO: DRY
                    // TODO: is this all even needed?
                    var pendingSelectorParameter = pendingSelector.Parameters[0];
                    //var selector = Expression.Lambda(currentParameter, currentParameter);

                    var combinedSelector = ExpressionExtensions.CombineLambdas(appliedSelector, pendingSelector);
                    var compensatedSelector = combinedSelector;

                    var binder = new NavigationPropertyBindingExpressionVisitor2(
                        pendingSelectorParameter,
                        transparentIdentifierAccessorMapping,
                        entityTypeAccessorMapping);

                    var boundSelector = binder.Visit(compensatedSelector);

                    var etamg = new EntityTypeAccessorMappingGenerator(
                        pendingSelectorParameter,
                        navigationExpansionExpression.TransparentIdentifierAccessorMapping,
                        navigationExpansionExpression.EntityTypeAccessorMapping);

                    etamg.Visit(boundSelector);

                    var nrev = new NavigationReplacingExpressionVisitor2(
                        pendingSelectorParameter,
                        pendingSelectorParameter,
                        navigationExpansionExpression.TransparentIdentifierAccessorMapping);

                    var newSelector = nrev.Visit(compensatedSelector);

                    var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                        pendingSelectorParameter.Type,
                        ((LambdaExpression)newSelector).Body.Type);

                    var result = Expression.Call(selectorMethodInfo, navigationExpansionExpression.Operand, newSelector);

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

                    //currentParameter = Expression.Parameter(newSelector.UnwrapQuote().Body.Type, "distinct"); // TODO: fix this
                    combinedSelector = ExpressionExtensions.CombineLambdas(appliedSelector, pendingSelector);

                    currentParameter = null;
                    appliedSelector = combinedSelector;
                    pendingSelector = null;
                    //firstSelectorParameter = null;
                    transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
                    entityTypeAccessorMapping = etamg.NewEntityAccessorMapping;
                    foundNavigations = new List<NavigationTreeNode>();
                    finalProjectionPath = new List<string>();


























                    //// TODO: ??????
                    //var currentParameter = compensatedSelector.Parameters[0];

                    //var compensatedSelector = ExpressionExtensions.CombineLambdas(combinedSelector, selector);

                    //var nrev = new NavigationReplacingExpressionVisitor(
                    //    _model,
                    //    navigationExpansionExpression.CurrentParameter,
                    //    firstSelectorParameter ?? navigationExpansionExpression.CurrentParameter,// <- TODO is this correct????                rootParameter,//selectors.FirstOrDefault()?.Parameters[0],
                    //    navigationExpansionExpression.CurrentParameter,
                    //    navigationExpansionExpression.TransparentIdentifierAccessorMapping,
                    //    navigationExpansionExpression.EntityTypeAccessorMapping);

                    //var nrev = new NavigationReplacingExpressionVisitor2(
                    //    currentParameter,
                    //    currentParameter,
                    //    //navigationExpansionExpression.CurrentParameter,
                    //    //navigationExpansionExpression.CurrentParameter,
                    //    navigationExpansionExpression.TransparentIdentifierAccessorMapping);

                    //var newSelector = nrev.Visit(compensatedSelector);

                    //var selectorMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                    //    currentParameter.Type,
                    //    //navigationExpansionExpression.CurrentParameter.Type,
                    //    ((LambdaExpression)newSelector).Body.Type);

                    //var etamg = new EntityTypeAccessorMappingGenerator(
                    //    currentParameter,//firstSelectorParameter ?? currentParameter, // navigationExpansionExpression.CurrentParameter,
                    //    navigationExpansionExpression.TransparentIdentifierAccessorMapping,
                    //    navigationExpansionExpression.EntityTypeAccessorMapping);

                    //etamg.Visit(compensatedSelector);

                    //var result = Expression.Call(selectorMethodInfo, navigationExpansionExpression.Operand, newSelector);

                    //if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableDistinctMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableAny))
                    //{
                    //    source = methodCallExpression.Update(methodCallExpression.Object, new[] { result });
                    //}
                    //else if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableTakeMethodInfo)
                    //    || methodCallExpression.Method.MethodIsClosedFormOf(QueryableContains))
                    //{
                    //    // TODO: is it necessary to visit the argument, or can we just pass it as is?
                    //    var newArgument = Visit(methodCallExpression.Arguments[1]);

                    //    source = methodCallExpression.Update(methodCallExpression.Object, new[] { result, newArgument });
                    //}
                    //else
                    //{
                    //    throw new InvalidOperationException("Unsupported method " + methodCallExpression.Method.Name);
                    //}

                    //currentParameter = Expression.Parameter(newSelector.UnwrapQuote().Body.Type, "distinct"); // TODO: fix this
                    //combinedSelector = ExpressionExtensions.CombineLambdas(appliedSelector, pendingSelector);

                    //currentParameter = null;
                    //appliedSelector = pendingSelector;
                    //pendingSelector = null;
                    ////firstSelectorParameter = null;
                    //transparentIdentifierAccessorMapping = new List<(List<INavigation> from, List<string> to)>();
                    //entityTypeAccessorMapping = etamg.NewEntityAccessorMapping;
                    //foundNavigations = new List<NavigationTreeNode>();
                    //finalProjectionPath = new List<string>();
                }
                else
                {
                    // TODO: need to run thru Expression.Update?
                    source = methodCallExpression;
                }

                return new NavigationExpansionExpression(
                    source,
                    //firstSelectorParameter,
                    currentParameter,
                    transparentIdentifierAccessorMapping,
                    entityTypeAccessorMapping,
                    appliedSelector,
                    pendingSelector,
                    foundNavigations,
                    finalProjectionPath,
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

                var entityTypeAccessorMapping = new List<(List<string> path, IEntityType entityType)>();
                entityTypeAccessorMapping.Add((new List<string>(), entityType));

                var result = new NavigationExpansionExpression(
                    operand: constantExpression,
                    //firstSelectorParameter: null,
                    currentParameter: null,
                    transparentIdentifierAccessorMapping: new List<(List<INavigation> from, List<string> to)>(),
                    entityTypeAccessorMapping: entityTypeAccessorMapping,
                    appliedSelector: null,
                    pendingSelector: null,
                    foundNavigations: new List<NavigationTreeNode>(),
                    finalProjectionPath: new List<string>(),
                    returnType: constantExpression.Type);

                return result;
            }
           
            return base.VisitConstant(constantExpression);
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

        private (Expression source, ParameterExpression parameter, LambdaExpression pendingSelector)  AddNavigationJoin(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            NavigationTreeNode navigationTree,
            List<INavigation> navigationPath,
            List<string> finalProjectionPath,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
            LambdaExpression pendingSelector)
        {
            var path = navigationTree.GeneratePath();

            if (!transparentIdentifierAccessorMapping.Any(m => m.from.Count == path.Count && m.from.Zip(path, (o, i) => o.Name == i).All(r => r)))
            {
                var navigation = navigationTree.Navigation;
                var sourceType = sourceExpression.Type.GetGenericArguments()[0];

                // is this the right way to get EntityTypes?
                var navigationTargetEntityType = navigation.IsDependentToPrincipal()
                    ? navigation.ForeignKey.PrincipalEntityType
                    : navigation.ForeignKey.DeclaringEntityType;

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var transparentIdentifierAccessorPath = transparentIdentifierAccessorMapping.Where(
                    m => m.from.Count == navigationPath.Count
                        && m.from.Zip(navigationPath, (o, i) => o == i).All(r => r)).SingleOrDefault().to;

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                //var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, transparentIdentifierAccessorPath);

                var accessorPath = transparentIdentifierAccessorPath != null
                    ? navigationTree.InitialPath.Concat(transparentIdentifierAccessorPath).ToList()
                    : navigationTree.InitialPath;

                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, accessorPath);

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

                if (navigationPath.Count == 0
                    && !transparentIdentifierAccessorMapping.Any(m => m.from.Count == 0))
                {
                    transparentIdentifierAccessorMapping.Add((from: navigationPath.ToList(), to: new List<string>()));
                }

                foreach (var transparentIdentifierAccessorMappingElement in transparentIdentifierAccessorMapping)
                {
                    transparentIdentifierAccessorMappingElement.to.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));

                    // in case of GroupJoin (optional navigation) source is hidden deeps since we also project the grouping
                    // we could remove the grouping in the future, but for nowe we need the grouping to properly recognize the LOJ pattern
                    if (navigationTree.Optional)
                    {
                        transparentIdentifierAccessorMappingElement.to.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    }
                }

                navigationPath.Add(navigation);
                transparentIdentifierAccessorMapping.Add((from: navigationPath.ToList(), to: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }));

                finalProjectionPath.Add("Outer");
                if (navigationTree.Optional)
                {
                    finalProjectionPath.Add("Outer");
                }

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
                result = AddNavigationJoin(
                    result.source,
                    result.parameter,
                    child,
                    navigationPath.ToList(),
                    finalProjectionPath,
                    transparentIdentifierAccessorMapping,
                    result.pendingSelector);
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

        // TODO: DRY
        private Expression BuildTransparentIdentifierAccessorExpression(Expression source, List<string> accessorPath)
        {
            var result = source;
            if (accessorPath != null)
            {
                foreach (var accessorPathElement in accessorPath)
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
        public List<string> InitialPath { get; set; }
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

        public static NavigationTreeNode Create(IEnumerable<INavigation> expansionPath, bool optional, IEnumerable<string> initialPath)
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
                InitialPath = initialPath.ToList(),
            };

            var child = Create(expansionPath.Skip(1), optional, new List<string>());
            if (child != null)
            {
                result.Children.Add(child);
                child.Parent = result;
            }

            return result;
        }

        public bool Contains(NavigationTreeNode other)
        {
            if (other.Navigation != Navigation
                || !other.InitialPath.SequenceEqual(InitialPath))
            {
                return false;
            }

            return other.Children.All(oc => Children.Any(c => c.Contains(oc)));
        }

        public bool TryCombine(NavigationTreeNode other)
        {
            if (other.Navigation != Navigation
                || !other.InitialPath.SequenceEqual(InitialPath))
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

        public List<NavigationTreeNode> FoundNavigations { get; }

        public NavigationFindingExpressionVisitor2(
            ParameterExpression sourceParameter,
            List<NavigationTreeNode> foundNavigations)
        {
            _sourceParameter = sourceParameter;
            FoundNavigations = foundNavigations;
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

                    var initialPath = GenerateInitialPath(navigationBindingExpression.RootParameter);

                    var navigationPath = NavigationTreeNode.Create(navigationBindingExpression.Navigations, inheritanceRoot, initialPath);
                    if (!FoundNavigations.Any(p => p.Contains(navigationPath)))
                    {
                        var success = false;
                        foreach (var foundNavigationPath in FoundNavigations)
                        {
                            if (!success)
                            {
                                success = foundNavigationPath.TryCombine(navigationPath);
                            }
                        }

                        if (!success)
                        {
                            FoundNavigations.Add(navigationPath);
                        }
                    }
                }

                return extensionExpression;
            }

            return base.VisitExtension(extensionExpression);
        }
    }

    //public class NavigationFindingExpressionVisitor : ExpressionVisitor
    //{
    //    private ParameterExpression _sourceParameter;
    //    private List<(List<INavigation> from, List<string> to)> _transparentIdentifierAccessorMapping;

    //    public List<NavigationPathNode> FoundNavigationPaths { get; }
    //    public List<(List<string> path, IEntityType entityType)> EntityTypeAccessorMapping { get; }

    //    public NavigationFindingExpressionVisitor(
    //        ParameterExpression sourceParameter,
    //        List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
    //        List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping,
    //        List<NavigationPathNode> foundNavigationPaths)
    //    {
    //        _sourceParameter = sourceParameter;
    //        _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
    //        EntityTypeAccessorMapping = entityTypeAccessorMapping;
    //        FoundNavigationPaths = foundNavigationPaths;
    //    }

    //    // TODO: clean up all this!!!!!!
    //    private List<string> GenerateInitialPath(Expression expression)
    //    {
    //        if (expression is ParameterExpression)
    //        {
    //            return new List<string>();
    //        }

    //        if (expression is MemberExpression memberExpression)
    //        {
    //            var innerPath = GenerateInitialPath(memberExpression.Expression);
    //            innerPath.Add(memberExpression.Member.Name);

    //            return innerPath;
    //        }

    //        // this probably should not be null, things will crash
    //        return null;
    //    }

    //    protected override Expression VisitMember(MemberExpression memberExpression)
    //    {
    //        var binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _sourceParameter, _transparentIdentifierAccessorMapping, EntityTypeAccessorMapping);
    //        if (/*binding.root == _sourceParameter
    //            && */binding.navigations.Any())
    //        {
    //            // TODO: inheritance?!
    //            var inheritanceRoot = binding.navigations[0].ClrType != binding.root.Type
    //                && binding.navigations[0].DeclaringEntityType.GetAllBaseTypes().Any(t => t.ClrType == binding.root.Type);

    //            var initialPath = GenerateInitialPath(binding.root);

    //            var navigationPath = NavigationPathNode.Create(binding.navigations, inheritanceRoot, initialPath);
    //            if (!FoundNavigationPaths.Any(p => p.Contains(navigationPath)))
    //            {
    //                var success = false;
    //                foreach (var foundNavigationPath in FoundNavigationPaths)
    //                {
    //                    if (!success)
    //                    {
    //                        success = foundNavigationPath.TryCombine(navigationPath);
    //                    }
    //                }

    //                if (!success)
    //                {
    //                    FoundNavigationPaths.Add(navigationPath);
    //                }
    //            }

    //            return memberExpression;
    //        }

    //        return base.VisitMember(memberExpression);
    //    }
    //}

    public class NavigationReplacingExpressionVisitor2 : ExpressionVisitor
    {
        private ParameterExpression _previousParameter;
        private ParameterExpression _newParameter;
        //private ParameterExpression _currentParameter;
        private List<(List<INavigation> from, List<string> to)> _transparentIdentifierAccessorMapping;
        //private List<(List<string> path, IEntityType entityType)> _entityTypeAccessorMapping;

        public NavigationReplacingExpressionVisitor2(
            ParameterExpression previousParameter,
            ParameterExpression newParameter,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping)
        {
            //_originalParameter = originalParameter;
            _previousParameter = previousParameter;
            _newParameter = newParameter;
            //_currentParameter = currentParameter;

            _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
            //_entityTypeAccessorMapping = entityTypeAccessorMapping;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression2 navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _previousParameter)
                    //&& navigationBindingExpression.Navigations.Count > 0)
                {
                    var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(
                        m => m.from.Count == navigationBindingExpression.Navigations.Count
                        && m.from.Zip(navigationBindingExpression.Navigations, (o, i) => o == i).All(e => e)).SingleOrDefault().to;

                    if (transparentIdentifierAccessorPath != null)
                    {
                        var result = BuildTransparentIdentifierAccessorExpression(_newParameter, transparentIdentifierAccessorPath);

                        return result;
                    }
                }
                
                //return navigationBindingExpression;
            }

            return base.VisitExtension(extensionExpression);
        }

        //protected override Expression VisitMember(MemberExpression memberExpression)
        //{
        //    var binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _rootParameter, _transparentIdentifierAccessorMapping, _entityTypeAccessorMapping);
        //    if (!binding.navigations.Any())
        //    {
        //        binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _originalParameter, _transparentIdentifierAccessorMapping, _entityTypeAccessorMapping);
        //    }

        //    if (/*(binding.root == _rootParameter || binding.root == _originalParameter)
        //        && */binding.navigations.Any())
        //    {
        //        var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(
        //            m => m.from.Count == binding.navigations.Count
        //            && m.from.Zip(binding.navigations/*.Select(p => p.Name)*/, (o, i) => o == i).All(e => e)).SingleOrDefault().to;

        //        if (transparentIdentifierAccessorPath != null)
        //        {
        //            var result = BuildTransparentIdentifierAccessorExpression(_currentParameter, transparentIdentifierAccessorPath);

        //            return result;
        //        }
        //    }

        //    return base.VisitMember(memberExpression);
        //}

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

        //protected override Expression VisitParameter(ParameterExpression parameterExpression)
        //{
        //    if (parameterExpression == _rootParameter
        //        || parameterExpression == _originalParameter)
        //    {
        //        var transparentIdentifierRootPath = _transparentIdentifierAccessorMapping.Where(m => m.from.Count == 0).SingleOrDefault().to;

        //        return BuildTransparentIdentifierAccessorExpression(_currentParameter, transparentIdentifierRootPath);
        //    }

        //    return parameterExpression;
        //}

        // TODO: DRY
        private Expression BuildTransparentIdentifierAccessorExpression(Expression source, List<string> accessorPath)
        {
            var result = source;
            if (accessorPath != null)
            {
                foreach (var accessorPathElement in accessorPath)
                {
                    result = Expression.Field(result, accessorPathElement);
                }
            }

            return result;
        }
    }




    //public class NavigationReplacingExpressionVisitor : ExpressionVisitor
    //{
    //    private IModel _model;
    //    private ParameterExpression _originalParameter;
    //    private ParameterExpression _rootParameter;
    //    private ParameterExpression _currentParameter;
    //    private List<(List<INavigation> from, List<string> to)> _transparentIdentifierAccessorMapping;
    //    private List<(List<string> path, IEntityType entityType)> _entityTypeAccessorMapping;

    //    public NavigationReplacingExpressionVisitor(
    //        IModel model,
    //        ParameterExpression originalParameter,
    //        ParameterExpression rootParameter,
    //        ParameterExpression currentParameter,
    //        List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
    //        List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping)
    //    {
    //        _model = model;
    //        _originalParameter = originalParameter;
    //        _rootParameter = rootParameter;
    //        _currentParameter = currentParameter;

    //        //_sourceParameter = sourceParameter;
    //        //_previousSelectorParameter = previousSelectorParameter;
    //        //_transparentIdentifierParameter = transparentIdentifierParameter;
    //        _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
    //        _entityTypeAccessorMapping = entityTypeAccessorMapping;
    //    }

    //    protected override Expression VisitMember(MemberExpression memberExpression)
    //    {
    //        var binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _rootParameter, _transparentIdentifierAccessorMapping, _entityTypeAccessorMapping);
    //        if (!binding.navigations.Any())
    //        {
    //            binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _originalParameter, _transparentIdentifierAccessorMapping, _entityTypeAccessorMapping);
    //        }

    //        if (/*(binding.root == _rootParameter || binding.root == _originalParameter)
    //            && */binding.navigations.Any())
    //        {
    //            var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(
    //                m => m.from.Count == binding.navigations.Count
    //                && m.from.Zip(binding.navigations/*.Select(p => p.Name)*/, (o, i) => o == i).All(e => e)).SingleOrDefault().to;

    //            if (transparentIdentifierAccessorPath != null)
    //            {
    //                var result = BuildTransparentIdentifierAccessorExpression(_currentParameter, transparentIdentifierAccessorPath);

    //                return result;
    //            }
    //        }

    //        return base.VisitMember(memberExpression);
    //    }

    //    protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
    //    {
    //        var newParameters = new List<ParameterExpression>();
    //        var parameterChanged = false;

    //        foreach (var parameter in lambdaExpression.Parameters)
    //        {
    //            if (parameter == _originalParameter)
    //            {
    //                newParameters.Add(_currentParameter);
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
    //    {
    //        if (parameterExpression == _rootParameter
    //            || parameterExpression == _originalParameter)
    //        {
    //            var transparentIdentifierRootPath = _transparentIdentifierAccessorMapping.Where(m => m.from.Count == 0).SingleOrDefault().to;

    //            return BuildTransparentIdentifierAccessorExpression(_currentParameter, transparentIdentifierRootPath);
    //        }

    //        return parameterExpression;
    //    }

    //    // TODO: DRY
    //    private Expression BuildTransparentIdentifierAccessorExpression(Expression source, List<string> accessorPath)
    //    {
    //        var result = source;
    //        if (accessorPath != null)
    //        {
    //            foreach (var accessorPathElement in accessorPath)
    //            {
    //                result = Expression.Field(result, accessorPathElement);
    //            }
    //        }

    //        return result;
    //    }
    //}
}
