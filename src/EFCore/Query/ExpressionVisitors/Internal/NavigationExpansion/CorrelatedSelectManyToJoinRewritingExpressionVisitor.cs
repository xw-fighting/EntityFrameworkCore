// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Extensions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class CorrelatedSelectManyToJoinRewritingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
            {
                var outer = Visit(methodCallExpression.Arguments[0]);
                var collectionSelector = Visit(methodCallExpression.Arguments[1]);
                var collectionSelectorLambda = collectionSelector.UnwrapQuote();

                var resultSelector = Visit(methodCallExpression.Arguments[2]);
                var resultSelectorLambda = resultSelector.UnwrapQuote();

                var correlationChecker = new CorrelationChecker(collectionSelectorLambda.Parameters[0]);
                correlationChecker.Visit(collectionSelectorLambda.Body);
                if (correlationChecker.Correlated)
                {
                    var correlationPredicateExtractor = new CorrelationPredicateExtractingExpressionVisitor(collectionSelectorLambda.Parameters[0]);
                    var collectionSelectorWithoutCorelationPredicate = correlationPredicateExtractor.Visit(collectionSelectorLambda.Body);
                    if (correlationPredicateExtractor.CorrelationPredicate != null)
                    {
                        var outerKeyLambdaBody = correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Left;
                        var outerKeyLambda = Expression.Lambda(outerKeyLambdaBody, collectionSelectorLambda.Parameters[0]);
                        var innerKeyLambda = Expression.Lambda(correlationPredicateExtractor.CorrelationPredicate.EqualExpression.Right, correlationPredicateExtractor.CorrelatedCollectionParameter);

                        var outerElementType = methodCallExpression.Method.GetGenericArguments()[0];
                        var innerElementType = methodCallExpression.Method.GetGenericArguments()[1];

                        var joinMethodInfo = QueryableJoinMethodInfo.MakeGenericMethod(
                            outerElementType,
                            innerElementType,
                            outerKeyLambdaBody.Type,
                            resultSelectorLambda.Body.Type);

                        var rewritten = Expression.Call(
                            joinMethodInfo,
                            outer,
                            collectionSelectorWithoutCorelationPredicate,
                            outerKeyLambda,
                            innerKeyLambda,
                            resultSelectorLambda);

                        return rewritten;
                    }
                }
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private class CorrelationChecker : NavigationExpansionExpressionVisitorBase
        {
            private ParameterExpression _rootParameter;

            public bool Correlated { get; private set; } = false;

            public CorrelationChecker(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
            {
                if (parameterExpression == _rootParameter)
                {
                    Correlated = true;
                }

                return parameterExpression;
            }
        }
    }
}
