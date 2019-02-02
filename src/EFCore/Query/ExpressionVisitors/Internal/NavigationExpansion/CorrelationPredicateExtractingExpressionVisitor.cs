// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    // TODO: can this be combined with the checker?
    public class CorrelationPredicateExtractingExpressionVisitor : NavigationExpansionExpressionVisitorBase
    {
        private ParameterExpression _rootParameter;

        public NullSafeEqualExpression CorrelationPredicate { get; private set; }
        public ParameterExpression CorrelatedCollectionParameter { get; private set; }

        public CorrelationPredicateExtractingExpressionVisitor(ParameterExpression rootParameter)
        {
            _rootParameter = rootParameter;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            // TODO: use method infos instead of method name
            if (methodCallExpression.Method.Name == "Where"
                && methodCallExpression.Arguments[1].UnwrapQuote() is LambdaExpression lambda
                && lambda.Body is NullSafeEqualExpression nullSafeEqualExpression)
            {
                var parameterFinder = new ParameterFinder(_rootParameter);
                parameterFinder.Visit(nullSafeEqualExpression.EqualExpression.Left);
                if (parameterFinder.Found)
                {
                    CorrelationPredicate = nullSafeEqualExpression;
                    CorrelatedCollectionParameter = methodCallExpression.Arguments[1].UnwrapQuote().Parameters[0];

                    return methodCallExpression.Update(
                        null,
                        new[]
                        {
                                methodCallExpression.Arguments[0],
                                Expression.Lambda(Expression.Constant(true), lambda.Parameters[0])
                        });
                }
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private class ParameterFinder : NavigationExpansionExpressionVisitorBase
        {
            private ParameterExpression _rootParameter;

            public bool Found { get; private set; } = false;

            public ParameterFinder(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
            {
                if (parameterExpression == _rootParameter)
                {
                    Found = true;
                }

                return parameterExpression;
            }
        }
    }
}
