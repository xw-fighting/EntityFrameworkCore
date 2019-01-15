// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.EntityFrameworkCore.Query.Pipeline
{
    public class QueryOptimizer
    {
        private readonly QueryCompilationContext2 _queryCompilationContext;

        public QueryOptimizer(QueryCompilationContext2 queryCompilationContext)
        {
            _queryCompilationContext = queryCompilationContext;
        }

        public Expression Visit(Expression query)
        {
            query = new EFQueryMetadataExpressionVisitor(_queryCompilationContext).Visit(query);
            query = new NullCheckRemovingExpressionVisitor().Visit(query);
            return query;
        }
    }

    public class EFQueryMetadataExpressionVisitor : ExpressionVisitor
    {
        private readonly QueryCompilationContext2 _queryCompilationContext;

        public EFQueryMetadataExpressionVisitor(QueryCompilationContext2 queryCompilationContext)
        {
            _queryCompilationContext = queryCompilationContext;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            var method = methodCallExpression.Method;
            if (method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && method.IsGenericMethod)
            {
                var genericMethodDefinition = method.GetGenericMethodDefinition();
                if (genericMethodDefinition == EntityFrameworkQueryableExtensions.AsTrackingMethodInfo
                    || genericMethodDefinition == EntityFrameworkQueryableExtensions.AsNoTrackingMethodInfo)
                {
                    var innerQueryable = Visit(methodCallExpression.Arguments[0]);
                    _queryCompilationContext.TrackQueryResults
                        = genericMethodDefinition == EntityFrameworkQueryableExtensions.AsTrackingMethodInfo;

                    return innerQueryable;
                }
            }

            return base.VisitMethodCall(methodCallExpression);
        }
    }
}
