// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Extensions.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal
{
    public static class ExpressionExtenssions
    {
        public static Expression UnwrapQuote(this Expression expression)
            => expression is UnaryExpression unary && expression.NodeType == ExpressionType.Quote
            ? unary.Operand
            : expression;

        public static bool IsIncludeMethod(this MethodCallExpression methodCallExpression)
            => methodCallExpression.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && methodCallExpression.Method.Name == nameof(EntityFrameworkQueryableExtensions.Include);
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


    public class NavigationPropertyBinder
    {
        // TODO: model should not be needed - entity type information will be embeded in the lambda
        public static (Expression root, IReadOnlyList<INavigation> navigations) BindNavigationProperties(Expression expression, IModel model)
        {
            var visitor = new NavigationPropertyBindingExpressionVisitor(model);
            var result = visitor.Visit(expression);

            return result is NavigationBindingExpression navigationBindingExpression
                ? (source: navigationBindingExpression.Root, navigations: navigationBindingExpression.Navigations)
                : (source: expression, navigations: new List<INavigation>().AsReadOnly());
        }

        private class NavigationPropertyBindingExpressionVisitor : ExpressionVisitor
        {
            private IModel _model;

            public NavigationPropertyBindingExpressionVisitor(IModel model)
            {
                _model = model;
            }

            public (Expression root, IReadOnlyList<INavigation> navigations) BindNavigationProperties(Expression expression)
            {
                var result = Visit(expression);

                return result is NavigationBindingExpression navigationBindingExpression
                    ? (source: navigationBindingExpression.Root, navigations: navigationBindingExpression.Navigations)
                    : (source: expression, navigations: new List<INavigation>().AsReadOnly());
            }

            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                var newExpression = Visit(memberExpression.Expression);

                var rootExpression = newExpression;

                // TODO: decorate lambda with model information and extract it from there
                var entityType = _model.FindEntityType(newExpression.Type);
                var navigations = new List<INavigation>();

                if (newExpression is NavigationBindingExpression navigationBindingExpression)
                {
                    rootExpression = navigationBindingExpression.Root;
                    entityType = navigationBindingExpression.Navigations.Last().GetTargetType();
                    navigations.AddRange(navigationBindingExpression.Navigations);
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

                return newExpression != memberExpression.Expression
                    ? memberExpression.Update(newExpression)
                    : memberExpression;
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

    public class NavigationExpandingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        private IModel _model;

        public NavigationExpandingExpressionVisitor(IModel model)
        {
            _model = model;
        }

        public virtual Expression ExpandNavigations(Expression expression)
        {
            var collectionNaviagtionRewritingExpressionVisitor = new CollectionNavigationRewritingExpressionVisitor(_model);
            var newExpression = collectionNaviagtionRewritingExpressionVisitor.Visit(expression);

            newExpression = Visit(newExpression);

            return newExpression;
        }

        private Expression ProcessSelect(MethodCallExpression methodCallExpression)
        {
            var source = Visit(methodCallExpression.Arguments[0]);
            var selector = Visit(methodCallExpression.Arguments[1]);
            var parameter = (selector.UnwrapQuote() as LambdaExpression).Parameters[0];
            var transparentIdentifierAccessorMapping = new List<(List<string> from, List<string> to)>();
            var previousSelector = default(LambdaExpression);
            var foundNavigations = new List<NavigationPathNode>();
            var finalProjectionPath = new List<string>();
            var newParameter = parameter;

            if (source is NavigationExpansionExpression navigationExpansionExpression)
            {
                source = navigationExpansionExpression.Operand;
                newParameter = navigationExpansionExpression.ParameterExpression;
                transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                previousSelector = navigationExpansionExpression.PreviousSelector;
                foundNavigations = navigationExpansionExpression.FoundNavigations;
                finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;
            }

            var pscev = new PreviousSelectorCompensatingExpressionVisitor(previousSelector, parameter);
            var compensatedSelector = pscev.Visit(selector);

            var nfev = new NavigationFindingExpressionVisitor(_model, previousSelector?.Parameters[0] ?? parameter, foundNavigations);
            nfev.Visit(compensatedSelector);

            var result = (source, parameter: newParameter);

            if (nfev.FoundNavigationPaths.Any())
            {
                foreach (var navigationPath in nfev.FoundNavigationPaths)
                {
                    result = AddNavigationJoin(
                        result.source,
                        result.parameter,
                        navigationPath,
                        new List<string>(),
                        finalProjectionPath,
                        transparentIdentifierAccessorMapping);
                }
            }

            var newSource = result.source;
            var nrev = new NavigationReplacingExpressionVisitor(
                _model,
                parameter,
                previousSelector?.Parameters[0],
                result.parameter,
                transparentIdentifierAccessorMapping);

            var newSelector = nrev.Visit(selector);

            //var newMapping = transparentIdentifierAccessorMapping.Select(m => (m.from, to: new List<string>(), found: false)).ToList();
            //var remapper = new Remapper(_model, parameter, newMapping);
            //remapper.Visit(selector);

            //transparentIdentifierAccessorMapping = newMapping.Where(m => m.found).Select(m => (m.from, m.to)).ToList();

            var newMethodInfo = QueryableSelectMethodInfo.MakeGenericMethod(
                result.parameter.Type,
                (selector.UnwrapQuote() as LambdaExpression).Body.Type);

            var rewritten = Expression.Call(newMethodInfo, newSource, newSelector);
            finalProjectionPath.Clear();

            previousSelector = (LambdaExpression)selector.UnwrapQuote();

            return new NavigationExpansionExpression(
                rewritten,
                result.parameter,
                transparentIdentifierAccessorMapping,
                previousSelector,
                foundNavigations,
                finalProjectionPath,
                methodCallExpression.Type);
        }

        private class Remapper : ExpressionVisitor
        {
            private IModel _model;
            private ParameterExpression _parameter;
            private List<(List<string> from, List<string> to, bool found)> _transparentIdentifierAccessorMapping;

            public Remapper(IModel model, ParameterExpression parameter, List<(List<string> from, List<string> to, bool found)> transparentIdentifierAccessorMapping)
            {
                _model = model;
                _parameter = parameter;
                _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
            }

            protected override Expression VisitNew(NewExpression newExpression)
            {
                for (var index = 0; index < newExpression.Arguments.Count; index++)
                {
                    var binding = NavigationPropertyBinder.BindNavigationProperties(newExpression.Arguments[index], _model);
                    if (binding.root == _parameter)
                    {
                        var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(
                            m => m.from.Count == binding.navigations.Count
                            && m.from.Zip(binding.navigations.Select(p => p.Name), (o, i) => o == i).All(e => e)).SingleOrDefault();

                        if (transparentIdentifierAccessorPath.to != null)
                        {
                            // TODO: improve this, maybe create data structure?
                            var to = new List<string>(transparentIdentifierAccessorPath.to);
                            to.Insert(0, newExpression.Members[index].Name);
                            _transparentIdentifierAccessorMapping.Add((transparentIdentifierAccessorPath.from, to, found: true));
                            _transparentIdentifierAccessorMapping.Remove(transparentIdentifierAccessorPath);
                        }
                    }
                }

                return newExpression;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
            {
                if (parameterExpression == _parameter)
                {
                    var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(m => m.from.Count == 0).SingleOrDefault();

                    if (transparentIdentifierAccessorPath.to != null)
                    {
                        transparentIdentifierAccessorPath.found = true;
                    }
                }

                return parameterExpression;
            }
        }

        private class PreviousSelectorCompensatingExpressionVisitor : ExpressionVisitor
        {
            private LambdaExpression _previousSelector;
            private ParameterExpression _parameterExpression;

            public PreviousSelectorCompensatingExpressionVisitor(LambdaExpression previousSelector, ParameterExpression parameterExpression)
            {
                _previousSelector = previousSelector;
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
                => parameterExpression == _parameterExpression && _previousSelector != null
                ? _previousSelector.Body
                : base.VisitParameter(parameterExpression);

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

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
            {
                return base.VisitMethodCall(methodCallExpression);
            }
        }

        protected override Expression VisitExtension(Expression extensionExpression)
            => extensionExpression;

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectMethodInfo))
            {
                var result = ProcessSelect(methodCallExpression);

                return result;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableWhereMethodInfo))
            {
                var source = Visit(methodCallExpression.Arguments[0]);
                var predicate = Visit(methodCallExpression.Arguments[1]);
                var parameter = (predicate.UnwrapQuote() as LambdaExpression).Parameters[0];
                var transparentIdentifierAccessorMapping = new List<(List<string> from, List<string> to)>();
                var previousSelector = default(LambdaExpression);
                var foundNavigations = new List<NavigationPathNode>();
                var finalProjectionPath = new List<string>();
                var newParameter = parameter;

                if (source is NavigationExpansionExpression navigationExpansionExpression)
                {
                    source = navigationExpansionExpression.Operand;
                    newParameter = navigationExpansionExpression.ParameterExpression;
                    transparentIdentifierAccessorMapping = navigationExpansionExpression.TransparentIdentifierAccessorMapping;
                    previousSelector = navigationExpansionExpression.PreviousSelector;
                    foundNavigations = navigationExpansionExpression.FoundNavigations;
                    finalProjectionPath = navigationExpansionExpression.FinalProjectionPath;
                }

                var pscev = new PreviousSelectorCompensatingExpressionVisitor(previousSelector, parameter);
                var compensatedPredicate = pscev.Visit(predicate);

                var nfev = new NavigationFindingExpressionVisitor(_model, previousSelector?.Parameters[0] ?? parameter, foundNavigations);
                nfev.Visit(compensatedPredicate);














                var result = (source, parameter: newParameter);

                if (nfev.FoundNavigationPaths.Any())
                {
                    foreach (var navigationPath in nfev.FoundNavigationPaths)
                    {
                        result = AddNavigationJoin(
                            result.source,
                            result.parameter,
                            navigationPath,
                            new List<string>(),
                            finalProjectionPath,
                            transparentIdentifierAccessorMapping);
                    }
                }

                var newSource = result.source;
                var nrev = new NavigationReplacingExpressionVisitor(
                    _model,
                    parameter,
                    previousSelector?.Parameters[0],
                    result.parameter,
                    transparentIdentifierAccessorMapping);

                var newPredicate = nrev.Visit(predicate);

                var newMethodInfo = QueryableWhereMethodInfo.MakeGenericMethod(result.parameter.Type);

                var rewritten = Expression.Call(newMethodInfo, newSource, newPredicate);

                return new NavigationExpansionExpression(
                    rewritten,
                    result.parameter,
                    transparentIdentifierAccessorMapping,
                    previousSelector,
                    foundNavigations,
                    finalProjectionPath,
                    methodCallExpression.Type);
            }

            return base.VisitMethodCall(methodCallExpression);
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

        private (Expression source, ParameterExpression parameter)  AddNavigationJoin(
            Expression sourceExpression,
            ParameterExpression parameterExpression,
            NavigationPathNode navigationPath,
            List<string> navigationPathNames,
            List<string> finalProjectionPath,
            List<(List<string> from, List<string> to)> transparentIdentifierAccessorMapping)
        {
            var path = navigationPath.GeneratePath();

            if (!transparentIdentifierAccessorMapping.Any(m => m.from.Count == path.Count && m.from.Zip(path, (o, i) => o == i).All(r => r)))
            {
                var navigation = navigationPath.Navigation;
                var sourceType = sourceExpression.Type.GetGenericArguments()[0];

                // is this the right way to get EntityTypes?
                var navigationTargetEntityType = navigation.IsDependentToPrincipal()
                    ? navigation.ForeignKey.PrincipalEntityType
                    : navigation.ForeignKey.DeclaringEntityType;

                var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(navigationTargetEntityType.ClrType);
                var resultType = typeof(TransparentIdentifier<,>).MakeGenericType(sourceType, navigationTargetEntityType.ClrType);

                var transparentIdentifierAccessorPath = transparentIdentifierAccessorMapping.Where(
                    m => m.from.Count == navigationPathNames.Count
                        && m.from.Zip(navigationPathNames, (o, i) => o == i).All(r => r)).SingleOrDefault().to;

                var outerParameter = Expression.Parameter(sourceType, parameterExpression.Name);
                var outerKeySelectorParameter = outerParameter;
                var transparentIdentifierAccessorExpression = BuildTransparentIdentifierAccessorExpression(outerParameter, transparentIdentifierAccessorPath);

                var outerKeySelectorBody = CreateKeyAccessExpression(
                    transparentIdentifierAccessorExpression,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.Properties
                        : navigation.ForeignKey.PrincipalKey.Properties,
                    addNullCheck: navigationPath.Optional);

                var innerKeySelectorParameterType = navigationTargetEntityType.ClrType;
                var innerKeySelectorParameter = Expression.Parameter(
                    innerKeySelectorParameterType,
                    parameterExpression.Name + "." + navigationPath.Navigation.Name);

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

                if (navigationPath.Optional)
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

                    var defaultIfEmptyMethodInfo = EnumerableDefaultIfEmpty.MakeGenericMethod(navigationTargetEntityType.ClrType);

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

                if (navigationPathNames.Count == 0
                    && !transparentIdentifierAccessorMapping.Any(m => m.from.Count == 0))
                {
                    transparentIdentifierAccessorMapping.Add((from: navigationPathNames.ToList(), to: new List<string>()));
                }

                foreach (var transparentIdentifierAccessorMappingElement in transparentIdentifierAccessorMapping)
                {
                    transparentIdentifierAccessorMappingElement.to.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));

                    // in case of GroupJoin (optional navigation) source is hidden deeps since we also project the grouping
                    // we could remove the grouping in the future, but for nowe we need the grouping to properly recognize the LOJ pattern
                    if (navigationPath.Optional)
                    {
                        transparentIdentifierAccessorMappingElement.to.Insert(0, nameof(TransparentIdentifier<object, object>.Outer));
                    }
                }

                navigationPathNames.Add(navigation.Name);
                transparentIdentifierAccessorMapping.Add((from: navigationPathNames.ToList(), to: new List<string> { nameof(TransparentIdentifier<object, object>.Inner) }));

                finalProjectionPath.Add("Outer");
                if (navigationPath.Optional)
                {
                    finalProjectionPath.Add("Outer");
                }
            }
            else
            {
                navigationPathNames.Add(navigationPath.Navigation.Name);
            }

            var result = (source: sourceExpression, parameter: parameterExpression);
            foreach (var child in navigationPath.Children)
            {
                result = AddNavigationJoin(
                    result.source,
                    result.parameter,
                    child,
                    navigationPathNames.ToList(),
                    finalProjectionPath,
                    transparentIdentifierAccessorMapping);
            }

            return result;
        }

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

    public class NavigationPathNode
    {
        public INavigation Navigation { get; set; }
        public bool Optional { get; set; }
        public NavigationPathNode Parent { get; set; }
        public List<NavigationPathNode> Children { get; set; }

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

        public static NavigationPathNode Create(IEnumerable<INavigation> expansionPath, bool optional)
        {
            if (expansionPath.Count() == 0)
            {
                return null;
            }

            var navigation = expansionPath.First();
            optional = optional || !navigation.ForeignKey.IsRequired || !navigation.IsDependentToPrincipal();
            var result = new NavigationPathNode
            {
                Navigation = navigation,
                Optional = optional,
                Children = new List<NavigationPathNode>(),
            };

            var child = Create(expansionPath.Skip(1), optional);
            if (child != null)
            {
                result.Children.Add(child);
                child.Parent = result;
            }

            return result;
        }

        public bool Contains(NavigationPathNode other)
        {
            if (other.Navigation != Navigation)
            {
                return false;
            }

            return other.Children.All(oc => Children.Any(c => c.Contains(oc)));
        }

        public bool TryCombine(NavigationPathNode other)
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

    public class NavigationFindingExpressionVisitor : ExpressionVisitor
    {
        private IModel _model;
        private ParameterExpression _sourceParameter;

        public List<NavigationPathNode> FoundNavigationPaths { get; }

        public NavigationFindingExpressionVisitor(
            IModel model,
            ParameterExpression sourceParameter,
            List<NavigationPathNode> foundNavigationPaths)
        {
            _model = model;
            _sourceParameter = sourceParameter;
            FoundNavigationPaths = foundNavigationPaths;
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _model);
            if (binding.root == _sourceParameter
                && binding.navigations.Any())
            {
                // TODO: inheritance?!
                var inheritanceRoot = binding.navigations[0].ClrType != binding.root.Type
                    && binding.navigations[0].DeclaringEntityType.GetAllBaseTypes().Any(t => t.ClrType == binding.root.Type);

                var navigationPath = NavigationPathNode.Create(binding.navigations, inheritanceRoot);
                if (!FoundNavigationPaths.Any(p => p.Contains(navigationPath)))
                {
                    var success = false;
                    foreach (var foundNavigationPath in FoundNavigationPaths)
                    {
                        if (!success)
                        {
                            success = foundNavigationPath.TryCombine(navigationPath);
                        }
                    }

                    if (!success)
                    {
                        FoundNavigationPaths.Add(navigationPath);
                    }
                }

                return memberExpression;
            }

            return base.VisitMember(memberExpression);
        }
    }

    public class NavigationReplacingExpressionVisitor : ExpressionVisitor
    {
        private IModel _model;
        private ParameterExpression _sourceParameter;
        private ParameterExpression _previousSelectorParameter;
        private ParameterExpression _transparentIdentifierParameter;
        private List<(List<string> from, List<string> to)> _transparentIdentifierAccessorMapping;

        public NavigationReplacingExpressionVisitor(
            IModel model,
            ParameterExpression sourceParameter,
            ParameterExpression previousSelectorParameter,
            ParameterExpression transparentIdentifierParameter,
            List<(List<string> from, List<string> to)> transparentIdentifierAccessorMapping)
        {
            _model = model;
            _sourceParameter = sourceParameter;
            _previousSelectorParameter = previousSelectorParameter;
            _transparentIdentifierParameter = transparentIdentifierParameter;
            _transparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var binding = NavigationPropertyBinder.BindNavigationProperties(memberExpression, _model);
            if ((binding.root == _sourceParameter || binding.root == _previousSelectorParameter)
                && binding.navigations.Any())
            {
                var transparentIdentifierAccessorPath = _transparentIdentifierAccessorMapping.Where(
                    m => m.from.Count == binding.navigations.Count
                    && m.from.Zip(binding.navigations.Select(p => p.Name), (o, i) => o == i).All(e => e)).SingleOrDefault().to;

                if (transparentIdentifierAccessorPath != null)
                {
                    var result = BuildTransparentIdentifierAccessorExpression(_transparentIdentifierParameter, transparentIdentifierAccessorPath);

                    return result;
                }
            }

            return base.VisitMember(memberExpression);
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newParameters = new List<ParameterExpression>();
            var parameterChanged = false;

            foreach (var parameter in lambdaExpression.Parameters)
            {
                if (parameter == _sourceParameter)
                {
                    newParameters.Add(_transparentIdentifierParameter);
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
            if (parameterExpression == _sourceParameter
                || parameterExpression == _previousSelectorParameter)
            {
                var transparentIdentifierRootPath = _transparentIdentifierAccessorMapping.Where(m => m.from.Count == 0).SingleOrDefault().to;

                return BuildTransparentIdentifierAccessorExpression(_transparentIdentifierParameter, transparentIdentifierRootPath);
            }

            return parameterExpression;
        }

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
}
