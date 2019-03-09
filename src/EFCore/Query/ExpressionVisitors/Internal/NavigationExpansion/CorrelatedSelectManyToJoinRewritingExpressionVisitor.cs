// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Extensions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class CorrelatedSelectManyToJoinRewritingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
            {
                var outer = Visit(methodCallExpression.Arguments[0]);
                var collectionSelector = Visit(methodCallExpression.Arguments[1]);
                var collectionSelectorLambda = collectionSelector.UnwrapQuote();

                var resultSelectorLambda = default(LambdaExpression);
                if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableSelectManyWithResultOperatorMethodInfo))
                {
                    var resultSelector = Visit(methodCallExpression.Arguments[2]);
                    resultSelectorLambda = resultSelector.UnwrapQuote();
                }
                else
                {
                    var collectionElementParameter1 = Expression.Parameter(methodCallExpression.Method.GetGenericArguments()[0], "o");
                    var collectionElementParameter2 = Expression.Parameter(methodCallExpression.Method.GetGenericArguments()[1], "i");
                    resultSelectorLambda = Expression.Lambda(collectionElementParameter2, collectionElementParameter1, collectionElementParameter2);
                }

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

                        return Expression.Call(
                            joinMethodInfo,
                            outer,
                            collectionSelectorWithoutCorelationPredicate,
                            outerKeyLambda,
                            innerKeyLambda,
                            resultSelectorLambda);
                    }
                }
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private class CorrelationChecker : NavigationExpansionExpressionVisitorBase
        {
            private ParameterExpression _rootParameter;
            private bool _correlated = false;
            private bool _defaultIfEmptyFound = false;

            public bool Correlated
            {
                get { return _correlated && !_defaultIfEmptyFound; }
            }

            public CorrelationChecker(ParameterExpression rootParameter)
            {
                _rootParameter = rootParameter;
            }

            protected override Expression VisitParameter(ParameterExpression parameterExpression)
            {
                if (parameterExpression == _rootParameter)
                {
                    _correlated = true;
                }

                return parameterExpression;
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
            {
                // TODO: perhaps be can be smarter here - if DefaultIfEmpty is only present in the deeper part of the query it might be OK to perform the optimization
                // as long as it doesn't affect the correlated collection directly
                if (methodCallExpression.Method.Name == nameof(Queryable.DefaultIfEmpty))
                {
                    _defaultIfEmptyFound = true;

                    return methodCallExpression;
                }

                return base.VisitMethodCall(methodCallExpression);
            }
        }
    }
}
