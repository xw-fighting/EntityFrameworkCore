// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public class LambdaCombiningVisitor : NavigationExpansionVisitorBase
    {
        private LambdaExpression _newSelector;
        private ParameterExpression _newParameter;
        private ParameterExpression _previousParameter;

        public LambdaCombiningVisitor(
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
                if (parameter == _previousParameter
                    && parameter != _newParameter)
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
}
