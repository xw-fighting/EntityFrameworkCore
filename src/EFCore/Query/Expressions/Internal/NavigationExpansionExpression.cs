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
    public class NavigationExpansionExpressionState
    {
        public ParameterExpression CurrentParameter { get; set; }
        public List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)> NavigationExpansionMapping = new List<(List<string> path, List<string> initialPath, IEntityType rootEntityType, List<INavigation> navigations)>();
        public LambdaExpression PendingSelector { get; set; }
        public List<NavigationTreeNode> FoundNavigations { get; set; } = new List<NavigationTreeNode>();
        public List<string> FinalProjectionPath { get; set; } = new List<string>();
    }

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
            if (State.FinalProjectionPath.Count == 0
                && (State.PendingSelector == null || State.PendingSelector.Body == State.PendingSelector.Parameters[0]))
            {
                return Operand;
            }

            // TODO: what is the correct order?

            var result = Operand;
            var parameter = Parameter(result.Type.GetGenericArguments()[0]);
            if (State.PendingSelector != null)
            {
                var pendingSelectMathod = _selectMethodInfo.MakeGenericMethod(parameter.Type, State.PendingSelector.Body.Type);
                result = Call(pendingSelectMathod, result, State.PendingSelector);
                parameter = Parameter(result.Type.GetGenericArguments()[0]);
            }
            else if (State.FinalProjectionPath.Count > 0)
            {
                // TODO: is this correct? do we only need to apply the path if no pending selector is present?
                // maybe this can be unified somehow?!

                //var parameter = Parameter(Operand.Type.GetGenericArguments()[0]);
                var body = (Expression)parameter;
                foreach (var finalProjectionPathElement in State.FinalProjectionPath)
                {
                    body = Field(body, finalProjectionPathElement);
                }

                var lambda = Lambda(body, parameter);
                var method = _selectMethodInfo.MakeGenericMethod(parameter.Type, body.Type);

                result = Call(method, Operand, lambda);
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
