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
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion;
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

            // TODO: hack to workaround type discrepancy that can happen sometimes when rerwriting collection navigations
            return newExpression.RemoveConvert();
        }

        private class ReducingVisitor : ExpressionVisitor
        {
            //protected override Expression VisitLambda<T>(Expression<T> lambdaExpression)
            //{
            //    var newBody = Visit(lambdaExpression.Body);

            //    return newBody != lambdaExpression.Body
            //        ? Expression.Lambda(newBody, lambdaExpression.Parameters)
            //        : lambdaExpression;
            //}
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
}
