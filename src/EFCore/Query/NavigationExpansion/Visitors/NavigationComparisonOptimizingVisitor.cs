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
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public class NavigationComparisonOptimizingVisitor : NavigationExpansionVisitorBase
    {
        private static readonly MethodInfo _objectEqualsMethodInfo
            = typeof(object).GetRuntimeMethod(nameof(object.Equals), new[] { typeof(object), typeof(object) });

        protected override Expression VisitBinary(BinaryExpression binaryExpression)
        {
            var newLeft = Visit(binaryExpression.Left);
            var newRight = Visit(binaryExpression.Right);

            if (binaryExpression.NodeType == ExpressionType.Equal
                || binaryExpression.NodeType == ExpressionType.NotEqual)
            {
                var rewritten = TryRewriteNavigationComparison(newLeft, newRight, equality: binaryExpression.NodeType == ExpressionType.Equal);
                if (rewritten != null)
                {
                    return rewritten;
                }
            }

            return binaryExpression.Update(newLeft, binaryExpression.Conversion, newRight);
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            Expression newLeft = null;
            Expression newRight = null;

            if (methodCallExpression.Method.Name == nameof(object.Equals)
                && methodCallExpression.Object != null
                && methodCallExpression.Arguments.Count == 1)
            {
                newLeft = Visit(methodCallExpression.Object);
                newRight = Visit(methodCallExpression.Arguments[0]);

                return TryRewriteNavigationComparison(newLeft, newRight, equality: true)
                    ?? methodCallExpression.Update(newLeft, new[] { newRight });
            }

            if (methodCallExpression.Method.Equals(_objectEqualsMethodInfo))
            {
                newLeft = methodCallExpression.Arguments[0];
                newRight = methodCallExpression.Arguments[1];

                return TryRewriteNavigationComparison(newLeft, newRight, equality: true)
                    ?? methodCallExpression.Update(null, new[] { newLeft, newRight });
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private Expression TryRewriteNavigationComparison(Expression left, Expression right, bool equality)
        {
            var leftBinding = left as NavigationBindingExpression;
            var rightBinding = right as NavigationBindingExpression;
            var leftNullConstant = left.IsNullConstantExpression();
            var rightNullConstant = right.IsNullConstantExpression();

            Expression newLeft = null;
            Expression newRight = null;

            // comparing two different collection navigations is always false
            if (leftBinding != null
                && rightBinding != null
                && leftBinding.NavigationTreeNode.Navigation != rightBinding.NavigationTreeNode.Navigation
                && (leftBinding.NavigationTreeNode.Navigation.IsCollection() || rightBinding.NavigationTreeNode.Navigation.IsCollection()))
            {
                if (leftBinding.NavigationTreeNode.Navigation.IsCollection())
                {
                    var parentTreeNode = leftBinding.NavigationTreeNode.Parent;
                    parentTreeNode.Children.Remove(leftBinding.NavigationTreeNode);
                }

                if (rightBinding.NavigationTreeNode.Navigation.IsCollection())
                {
                    var parentTreeNode = rightBinding.NavigationTreeNode.Parent;
                    parentTreeNode.Children.Remove(rightBinding.NavigationTreeNode);
                }

                return Expression.Constant(false);
            }

            if (leftBinding != null && rightBinding != null
                && leftBinding.EntityType == rightBinding.EntityType)
            {
                if (leftBinding.NavigationTreeNode.Navigation == rightBinding.NavigationTreeNode.Navigation
                    && leftBinding.NavigationTreeNode.Navigation?.IsCollection() == true)
                {
                    leftBinding = CreateParentBindingExpression(leftBinding);
                    rightBinding = CreateParentBindingExpression(rightBinding);
                }

                // TODO: what about entities without PKs?
                var primaryKeyProperties = leftBinding.EntityType.FindPrimaryKey().Properties;
                newLeft = NavigationExpansionHelpers.CreateKeyAccessExpression(leftBinding, primaryKeyProperties);
                newRight = NavigationExpansionHelpers.CreateKeyAccessExpression(rightBinding, primaryKeyProperties);
            }

            if (leftBinding != null
                && rightNullConstant)
            {
                if (leftBinding.NavigationTreeNode.Navigation?.IsCollection() == true)
                {
                    leftBinding = CreateParentBindingExpression(leftBinding);
                }

                // TODO: what about entities without PKs?
                var primaryKeyProperties = leftBinding.EntityType.FindPrimaryKey().Properties;
                newLeft = NavigationExpansionHelpers.CreateKeyAccessExpression(leftBinding, primaryKeyProperties);
                newRight = NavigationExpansionHelpers.CreateNullKeyExpression(newLeft.Type, primaryKeyProperties.Count);
            }

            if (rightBinding != null
                && leftNullConstant)
            {
                if (rightBinding.NavigationTreeNode.Navigation?.IsCollection() == true)
                {
                    rightBinding = CreateParentBindingExpression(rightBinding);
                }

                // TODO: what about entities without PKs?
                var primaryKeyProperties = rightBinding.EntityType.FindPrimaryKey().Properties;
                newRight = NavigationExpansionHelpers.CreateKeyAccessExpression(rightBinding, primaryKeyProperties);
                newLeft = NavigationExpansionHelpers.CreateNullKeyExpression(newRight.Type, primaryKeyProperties.Count);
            }

            if (newLeft == null || newRight == null)
            {
                return null;
            }

            if (newLeft.Type != newRight.Type)
            {
                if (newLeft.Type.IsNullableType())
                {
                    newRight = Expression.Convert(newRight, newLeft.Type);
                }
                else
                {
                    newLeft = Expression.Convert(newLeft, newRight.Type);
                }
            }

            return equality
                ? Expression.Equal(newLeft, newRight)
                : Expression.NotEqual(newLeft, newRight);
        }

        //// TODO: DRY - copied in many places
        //private static Expression CreateKeyAccessExpression(
        //    Expression target, IReadOnlyList<IProperty> properties, bool addNullCheck = false)
        //    => properties.Count == 1
        //        ? CreatePropertyExpression(target, properties[0], addNullCheck)
        //        : Expression.New(
        //            AnonymousObject.AnonymousObjectCtor,
        //            Expression.NewArrayInit(
        //                typeof(object),
        //                properties
        //                    .Select(p => Expression.Convert(CreatePropertyExpression(target, p, addNullCheck), typeof(object)))
        //                    .Cast<Expression>()
        //                    .ToArray()));

        //// TODO: DRY - copied in many places
        //private static Expression CreatePropertyExpression(Expression target, IProperty property, bool addNullCheck)
        //{
        //    var propertyExpression = target.CreateEFPropertyExpression(property, makeNullable: false);

        //    var propertyDeclaringType = property.DeclaringType.ClrType;
        //    if (propertyDeclaringType != target.Type
        //        && target.Type.GetTypeInfo().IsAssignableFrom(propertyDeclaringType.GetTypeInfo()))
        //    {
        //        if (!propertyExpression.Type.IsNullableType())
        //        {
        //            propertyExpression = Expression.Convert(propertyExpression, propertyExpression.Type.MakeNullable());
        //        }

        //        return Expression.Condition(
        //            Expression.TypeIs(target, propertyDeclaringType),
        //            propertyExpression,
        //            Expression.Constant(null, propertyExpression.Type));
        //    }

        //    return addNullCheck
        //        ? new NullConditionalExpression(target, propertyExpression)
        //        : propertyExpression;
        //}

        private NavigationBindingExpression CreateParentBindingExpression(NavigationBindingExpression navigationBindingExpression)
        {
            // TODO: idk if thats correct
            var parentNavigationEntityType = navigationBindingExpression.NavigationTreeNode.Navigation.FindInverse().GetTargetType();
            var parentTreeNode = navigationBindingExpression.NavigationTreeNode.Parent;
            parentTreeNode.Children.Remove(navigationBindingExpression.NavigationTreeNode);

            //var parentNavigationEntityType = parentTreeNode.Navigation?.DeclaringEntityType
            //    ?? navigationBindingExpression.SourceMapping.RootEntityType;

            return new NavigationBindingExpression(
                navigationBindingExpression.RootParameter,
                parentTreeNode,
                parentNavigationEntityType,
                navigationBindingExpression.SourceMapping,
                // TODO: is this correct? what about 1-1 navigations?
                parentNavigationEntityType.ClrType);
        }
    }
}
