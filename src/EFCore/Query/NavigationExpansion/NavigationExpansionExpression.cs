// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion
{
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
            if (!State.ApplyPendingSelector
                && State.PendingTerminatingOperator == null)
            {
                // TODO: hack to workaround type discrepancy that can happen sometimes when rerwriting collection navigations
                if (Operand.Type != _returnType)
                {
                    return Convert(Operand, _returnType);
                }

                return Operand;
            }

            var result = Operand;
            var parameter = Parameter(result.Type.GetSequenceType());
            if (State.ApplyPendingSelector)
            {
                var pendingSelector = (LambdaExpression)new NavigationPropertyUnbindingVisitor(State.CurrentParameter).Visit(State.PendingSelector);

                // we can't get body type using lambda.Body.Type because in some cases (SelectMany) we manually set the lambda type (IEnumerable<Entity>) where the body itself is IQueryable
                // TODO: this might be problem in other places!
                var pendingSelectorBodyType = pendingSelector.Type.GetGenericArguments()[1];

                var pendingSelectMathod = result.Type.IsGenericType && (result.Type.GetGenericTypeDefinition() == typeof(IEnumerable<>) || result.Type.GetGenericTypeDefinition() == typeof(IOrderedEnumerable<>))
                    ? _enumerableSelectMethodInfo.MakeGenericMethod(parameter.Type, pendingSelectorBodyType)
                    : _queryableSelectMethodInfo.MakeGenericMethod(parameter.Type, pendingSelectorBodyType);

                result = Call(pendingSelectMathod, result, pendingSelector);
                parameter = Parameter(result.Type.GetSequenceType());
            }

            if (State.PendingTerminatingOperator != null)
            {
                var terminatingOperatorMethodInfo = State.PendingTerminatingOperator.MakeGenericMethod(parameter.Type);

                result = Call(terminatingOperatorMethodInfo, result);
            }

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

            if (State.ApplyPendingSelector)
            {
                expressionPrinter.StringBuilder.Append(".PendingSelect(");
                expressionPrinter.Visit(State.PendingSelector);
                expressionPrinter.StringBuilder.Append(")");
            }

            if (State.PendingTerminatingOperator != null)
            {
                expressionPrinter.StringBuilder.Append(".Pending" + State.PendingTerminatingOperator.Name);
            }
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
