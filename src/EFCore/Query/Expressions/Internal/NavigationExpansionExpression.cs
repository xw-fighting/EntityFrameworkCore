// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.Expressions.Internal
{
    public class NavigationExpansionExpression : Expression, IPrintable
    {
        private MethodInfo _selectMethodInfo
            = typeof(Queryable).GetMethods().Where(m => m.Name == nameof(Queryable.Select) && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();

        private Type _returnType;

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override Type Type => _returnType;
        public override bool CanReduce => true;
        public override Expression Reduce()
        {
            if (FinalProjectionPath.Count == 0
                && PendingSelector == null)
            {
                return Operand;
            }

            // TODO: what is the correct order?

            var result = Operand;
            var parameter = Parameter(result.Type.GetGenericArguments()[0]);
            if (PendingSelector != null)
            {
                var pendingSelectMathod = _selectMethodInfo.MakeGenericMethod(parameter.Type, PendingSelector.Body.Type);
                result = Call(pendingSelectMathod, result, PendingSelector);
                parameter = Parameter(result.Type.GetGenericArguments()[0]);
            }

            if (FinalProjectionPath.Count > 0)
            {
                //    //var parameter = Parameter(Operand.Type.GetGenericArguments()[0]);
                //    var body = (Expression)parameter;
                //    foreach (var finalProjectionPathElement in FinalProjectionPath)
                //    {
                //        body = Field(body, finalProjectionPathElement);
                //    }

                //    var lambda = Lambda(body, parameter);
                //    var method = _selectMethodInfo.MakeGenericMethod(parameter.Type, body.Type);

                //    result = Call(method, Operand, lambda);
            }

            if (_returnType.IsGenericType && _returnType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>))
            { 
                var toOrderedMethodInfo = typeof(NavigationExpansionExpression).GetMethod(nameof(NavigationExpansionExpression.ToOrdered)).MakeGenericMethod(parameter.Type);

                result = Call(toOrderedMethodInfo, result);

                return result;
            }

            return result;
        }
            
        public Expression Operand { get; }

        public ParameterExpression FirstSelectorParameter { get; } // parameter of first selector - this will be the root of all the navigations after selectors are applied to the current lambda
        public ParameterExpression CurrentParameter { get; } // parameter of the current lambda 

        public List<(List<INavigation> from, List<string> to)> TransparentIdentifierAccessorMapping { get; }
        public List<(List<string> path, IEntityType entityType)> EntityTypeAccessorMapping { get; }

        public LambdaExpression AppliedSelector { get; }
        public LambdaExpression PendingSelector { get; }

        public List<NavigationTreeNode> FoundNavigations { get; }

        public List<string> FinalProjectionPath { get; }

        public NavigationExpansionExpression(
            Expression operand,
            ParameterExpression firstSelectorParameter,
            ParameterExpression currentParameter,
            List<(List<INavigation> from, List<string> to)> transparentIdentifierAccessorMapping,
            List<(List<string> path, IEntityType entityType)> entityTypeAccessorMapping,
            LambdaExpression appliedSelector,
            LambdaExpression pendingSelector,
            List<NavigationTreeNode> foundNavigations,
            List<string> finalProjectionPath,
            Type returnType)
        {
            Operand = operand;
            FirstSelectorParameter = firstSelectorParameter;
            CurrentParameter = currentParameter;
            TransparentIdentifierAccessorMapping = transparentIdentifierAccessorMapping;
            EntityTypeAccessorMapping = entityTypeAccessorMapping;
            AppliedSelector = appliedSelector;
            PendingSelector = pendingSelector;
            FoundNavigations = foundNavigations;
            FinalProjectionPath = finalProjectionPath;
            _returnType = returnType;
        }

        public void Print([NotNull] ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Print(Operand);
        }

        public static IOrderedQueryable<TElement> ToOrdered<TElement>(IQueryable<TElement> source)
            => new IOrderedQueryableAdapter<TElement>(source);

        private class IOrderedQueryableAdapter<TElement> : IOrderedQueryable<TElement>
        {
            IQueryable<TElement> _source;

            public IOrderedQueryableAdapter(IQueryable<TElement> source)
            {
                _source = source;
            }

            public Type ElementType => _source.ElementType;

            public Expression Expression => _source.Expression;

            public IQueryProvider Provider => _source.Provider;

            public IEnumerator<TElement> GetEnumerator()
                => _source.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator()
                => ((IEnumerable)_source).GetEnumerator();
        }
    }
}
