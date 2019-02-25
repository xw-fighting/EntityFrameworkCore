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
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.Expressions.Internal
{
    public class SourceMapping
    {
        public List<string> InitialPath { get; set; } = new List<string>();
        public IEntityType RootEntityType { get; set; }
        public List<NavigationTreeNode> FoundNavigations { get; set; } = new List<NavigationTreeNode>();

        public List<(List<string> path, List<INavigation> navigations)> TransparentIdentifierMapping { get; set; }
            = new List<(List<string> path, List<INavigation> navigations)>();
    }

    public class SourceMapping2
    {
        public IEntityType RootEntityType { get; set; }

        public NavigationTreeNode2 NavigationTree { get; set; }
    }

    public class NavigationExpansionExpressionState
    {
        public ParameterExpression CurrentParameter { get; set; }
        public List<SourceMapping> SourceMappings { get; set; } = new List<SourceMapping>();
        public List<SourceMapping2> SourceMappings2 { get; set; } = new List<SourceMapping2>();
        public LambdaExpression PendingSelector { get; set; }
        public LambdaExpression PendingSelector2 { get; set; }
        public bool ApplyPendingSelector { get; set; }
        public List<List<string>> CustomRootMappings { get; set; } = new List<List<string>>();
    }

    public class NavigationExpansionExpression : Expression, IPrintable
    {
        private MethodInfo _queryableSelectMethodInfo
            = typeof(Queryable).GetMethods().Where(m => m.Name == nameof(Queryable.Select) && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();

        private MethodInfo _enumerableSelectMethodInfo
            = typeof(Enumerable).GetMethods().Where(m => m.Name == nameof(Enumerable.Select) && m.GetParameters()[1].ParameterType.GetGenericArguments().Count() == 2).Single();

        private Type _returnType;

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override Type Type => _returnType;
        public override bool CanReduce => true;
        public override Expression Reduce()
        {
            if (!State.ApplyPendingSelector)
            {
                // TODO: hack to workaround type discrepancy that can happen sometimes when rerwriting collection navigations
                if (Operand.Type != _returnType)
                {
                    return Convert(Operand, _returnType);
                }

                return Operand;
            }

            var result = Operand;
            var parameter = Parameter(result.Type.GetGenericArguments()[0]);

            var unbinder = new NavigationPropertyUnbindingBindingExpressionVisitor2(State.CurrentParameter);
            var pendingSelector = (LambdaExpression)unbinder.Visit(State.PendingSelector2);

            var pendingSelectMathod = result.Type.IsGenericType && result.Type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                ? _enumerableSelectMethodInfo.MakeGenericMethod(parameter.Type, pendingSelector.Body.Type)
                : _queryableSelectMethodInfo.MakeGenericMethod(parameter.Type, pendingSelector.Body.Type);

            result = Call(pendingSelectMathod, result, pendingSelector);
            parameter = Parameter(result.Type.GetGenericArguments()[0]);

            if (_returnType.IsGenericType && _returnType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>))
            { 
                var toOrderedMethodInfo = typeof(NavigationExpansionExpression).GetMethod(nameof(NavigationExpansionExpression.ToOrdered)).MakeGenericMethod(parameter.Type);

                return Call(toOrderedMethodInfo, result);
            }

            return result;
        }
            
        public Expression Operand { get; }

        public NavigationExpansionExpressionState State { get; private set; }

        public NavigationExpansionExpression(
            Expression operand,
            NavigationExpansionExpressionState state,
            Type returnType)
        {
            Operand = operand;
            State = state;
            _returnType = returnType;
        }

        public void Print([NotNull] ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Visit(Operand);
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
