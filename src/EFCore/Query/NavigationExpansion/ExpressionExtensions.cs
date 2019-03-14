// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
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

            var lcev = new LambdaCombiningVisitor(first, first.Parameters[0], secondLambdaParameterToReplace);

            return (LambdaExpression)lcev.Visit(second);
        }

        // TODO: DRY this entire thing
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

            // TODO: DRY
            protected override Expression VisitExtension(Expression extensionExpression)
            {
                if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
                {
                    var newRootParameter = (ParameterExpression)Visit(navigationBindingExpression.RootParameter);

                    return newRootParameter != navigationBindingExpression.RootParameter
                        ? new NavigationBindingExpression(
                            newRootParameter,
                            navigationBindingExpression.NavigationTreeNode,
                            navigationBindingExpression.EntityType,
                            navigationBindingExpression.SourceMapping,
                            navigationBindingExpression.Type)
                        : navigationBindingExpression;
                }

                throw new InvalidOperationException("Unhandled extension expression: " + extensionExpression);
            }
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
}
