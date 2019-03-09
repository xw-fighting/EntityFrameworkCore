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
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal
{
    /// <summary>
    ///     Rewrites collection navigations into subqueries, e.g.:
    ///     customers.Select(c => c.Order.OrderDetails.Where(...)) => customers.Select(c => orderDetails.Where(od => od.OrderId == c.Order.Id).Where(...))
    /// </summary>
    public class CollectionNavigationRewritingExpressionVisitor2 : LinqQueryExpressionVisitorBase
    {
        private ParameterExpression _sourceParameter;
        private MethodInfo _listExistsMethodInfo = typeof(List<>).GetMethods().Where(m => m.Name == nameof(List<int>.Exists)).Single();

        public CollectionNavigationRewritingExpressionVisitor2(
            ParameterExpression sourceParameter)
        {
            _sourceParameter = sourceParameter;
        }

        protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
        {
            var newBody = Visit(lambdaExpression.Body);

            return newBody != lambdaExpression.Body
                ? Expression.Lambda(newBody, lambdaExpression.Parameters)
                : lambdaExpression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            // don't touch Include
            // this is temporary, new nav expansion happens to early at the moment
            if (methodCallExpression.IsIncludeMethod())
            {
                return methodCallExpression;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyMethodInfo))
            {
                return methodCallExpression;
            }

            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
            {
                var newResultSelector = Visit(methodCallExpression.Arguments[2]);

                return newResultSelector != methodCallExpression.Arguments[2]
                    ? methodCallExpression.Update(methodCallExpression.Object, new[] { methodCallExpression.Arguments[0], methodCallExpression.Arguments[1], newResultSelector })
                    : methodCallExpression;
            }

            // List<T>.Exists(predicate) -> Enumerable.Any<T>(source, predicate)
            if (methodCallExpression.Method.Name == nameof(List<int>.Exists)
                && methodCallExpression.Method.DeclaringType.IsGenericType
                && methodCallExpression.Method.DeclaringType.GetGenericTypeDefinition() == typeof(List<>))
            {
                var anyLambda = Expression.Lambda(
                    ((LambdaExpression)methodCallExpression.Arguments[0]).Body,
                    ((LambdaExpression)methodCallExpression.Arguments[0]).Parameters[0]);

                var result = Expression.Call(
                    EnumerableAnyPredicate.MakeGenericMethod(methodCallExpression.Method.DeclaringType.TryGetSequenceType()/*.GetGenericArguments()[0]*/),
                    methodCallExpression.Object,
                    anyLambda);

                return Visit(result);
            }

            // TODO: collection.Contains?

            return base.VisitMethodCall(methodCallExpression);
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                if (navigationBindingExpression.NavigationTreeNode.Parent != null
                    && navigationBindingExpression.NavigationTreeNode.Navigation is INavigation lastNavigation
                    && lastNavigation.IsCollection())
                {
                    var collectionNavigationElementType = lastNavigation.ForeignKey.DeclaringEntityType.ClrType;
                    var entityQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(collectionNavigationElementType);

                    navigationBindingExpression.NavigationTreeNode.Parent.Children.Remove(navigationBindingExpression.NavigationTreeNode);

                    //TODO: this could be other things too: EF.Property and maybe field
                    var outerBinding = new NavigationBindingExpression(
                    navigationBindingExpression.RootParameter,
                    navigationBindingExpression.NavigationTreeNode.Parent,
                    //navigationBindingExpression.NavigationTreeNode.Navigation.GetTargetType() ?? navigationBindingExpression.SourceMapping.RootEntityType,
                    lastNavigation.DeclaringEntityType,
                    navigationBindingExpression.SourceMapping,
                    lastNavigation.DeclaringEntityType.ClrType);

                    var outerKeyAccess = CreateKeyAccessExpression(
                        outerBinding,
                        lastNavigation.ForeignKey.PrincipalKey.Properties);

                    var innerParameter = Expression.Parameter(collectionNavigationElementType, collectionNavigationElementType.GenerateParameterName());
                    var innerKeyAccess = CreateKeyAccessExpression(
                        innerParameter,
                        lastNavigation.ForeignKey.Properties);

                    var predicate = Expression.Lambda(
                        CreateKeyComparisonExpressionForCollectionNavigationSubquery(
                            outerKeyAccess,
                            innerKeyAccess,
                            outerBinding,
                            navigationBindingExpression.RootParameter,
                            // TODO: this is hacky
                            navigationBindingExpression.NavigationTreeNode.NavigationChain()),
                        innerParameter);

                    //predicate = (LambdaExpression)new NavigationPropertyUnbindingBindingExpressionVisitor(navigationBindingExpression.RootParameter).Visit(predicate);

                    var result = Expression.Call(
                        QueryableWhereMethodInfo.MakeGenericMethod(collectionNavigationElementType),
                        entityQueryable,
                        predicate);

                    return result;
                }
            }

            if (extensionExpression is NullSafeEqualExpression nullSafeEqualExpression)
            {
                var newOuterKeyNullCheck = Visit(nullSafeEqualExpression.OuterKeyNullCheck);
                var newEqualExpression = (BinaryExpression)Visit(nullSafeEqualExpression.EqualExpression);
                var newNavigationRootExpression = Visit(nullSafeEqualExpression.NavigationRootExpression);

                if (newOuterKeyNullCheck != nullSafeEqualExpression.OuterKeyNullCheck
                    || newEqualExpression != nullSafeEqualExpression.EqualExpression
                    || newNavigationRootExpression != nullSafeEqualExpression.NavigationRootExpression)
                {
                    return new NullSafeEqualExpression(newOuterKeyNullCheck, newEqualExpression, newNavigationRootExpression, nullSafeEqualExpression.Navigations);
                }
            }

            if (extensionExpression is NavigationExpansionExpression nee)
            {
                var newOperand = Visit(nee.Operand);
                if (newOperand != nee.Operand)
                {
                    return new NavigationExpansionExpression(newOperand, nee.State, nee.Type);
                }
            }

            return extensionExpression;

            //// TODO: just return for all other expression also? - we probably don't want to reduce at this point
            //if (extensionExpression is NavigationExpansionExpression)
            //{
            //    return extensionExpression;
            //}

            //return base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var newExpression = Visit(memberExpression.Expression);
            if (newExpression != memberExpression.Expression)
            {
                if (memberExpression.Member.Name == nameof(List<int>.Count))
                {
                    // TODO: what about custom collection?????????? - how do we get type argument there
                    var countMethod = QueryableCountMethodInfo.MakeGenericMethod(newExpression.Type.TryGetSequenceType()/*.GetGenericArguments()[0]*/);
                    var result = Expression.Call(instance: null, countMethod, newExpression);

                    return result;
                }
                else
                {
                    return memberExpression.Update(newExpression);
                }
            }

            return memberExpression;
        }

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

        private static Expression CreateKeyComparisonExpressionForCollectionNavigationSubquery(
            Expression outerKeyExpression,
            Expression innerKeyExpression,
            Expression colectionRootExpression,
            Expression navigationRootExpression,
            IEnumerable<INavigation> navigations)
        {
            if (outerKeyExpression.Type != innerKeyExpression.Type)
            {
                if (outerKeyExpression.Type.IsNullableType())
                {
                    Debug.Assert(outerKeyExpression.Type.UnwrapNullableType() == innerKeyExpression.Type);

                    innerKeyExpression = Expression.Convert(innerKeyExpression, outerKeyExpression.Type);
                }
                else
                {
                    Debug.Assert(innerKeyExpression.Type.IsNullableType());
                    Debug.Assert(innerKeyExpression.Type.UnwrapNullableType() == outerKeyExpression.Type);

                    outerKeyExpression = Expression.Convert(outerKeyExpression, innerKeyExpression.Type);
                }
            }

            var outerNullProtection
                = Expression.NotEqual(
                    colectionRootExpression,
                    Expression.Constant(null, colectionRootExpression.Type));

            return new NullSafeEqualExpression(
                outerNullProtection,
                Expression.Equal(outerKeyExpression, innerKeyExpression),
                navigationRootExpression,
                navigations.ToList());
        }
    }
}
