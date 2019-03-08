// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Extensions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class QueryMethodSimplifyingExpressionVisitor : LinqQueryExpressionVisitorBase
    {
        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.MethodIsClosedFormOf(QueryableCountPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableFirstOrDefaultPredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSinglePredicateMethodInfo)
                || methodCallExpression.Method.MethodIsClosedFormOf(QueryableSingleOrDefaultPredicateMethodInfo))
            {
                return SimplifyPredicate(methodCallExpression, queryable: true);
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private MethodCallExpression SimplifyPredicate(MethodCallExpression methodCallExpression, bool queryable)
        {
            var newSource = Visit(methodCallExpression.Arguments[0]);

            var whereMethodInfo = queryable
                ? QueryableWhereMethodInfo
                : EnumerableWhereMethodInfo;

            var typeArgument = newSource.Type.GetGenericArguments()[0];
            whereMethodInfo = whereMethodInfo.MakeGenericMethod(typeArgument);

            var whereMethodCall = Expression.Call(whereMethodInfo, newSource, methodCallExpression.Arguments[1]);

            var newMethodInfo = GetNewMethodInfo(methodCallExpression.Method.Name, queryable);
            newMethodInfo = newMethodInfo.MakeGenericMethod(typeArgument);

            var result = Expression.Call(newMethodInfo, whereMethodCall);

            return result;
        }

        private MethodInfo GetNewMethodInfo(string name, bool queryable)
        {
            if (queryable)
            {
                switch (name)
                {
                    case nameof(Queryable.Count):
                        return QueryableCountMethodInfo;

                    case nameof(Queryable.First):
                        return QueryableFirstMethodInfo;

                    case nameof(Queryable.FirstOrDefault):
                        return QueryableFirstOrDefaultMethodInfo;

                    case nameof(Queryable.Single):
                        return QueryableSingleMethodInfo;

                    case nameof(Queryable.SingleOrDefault):
                        return QueryableSingleOrDefaultMethodInfo;
                }
            }
            else
            {
                //switch (name)
                //{
                //    case nameof(Enumerable.Count):
                //        return EnumerableCountMethodInfo;

                //    case nameof(Enumerable.First):
                //        return EnumerableFirstMethodInfo;

                //    case nameof(Enumerable.FirstOrDefault):
                //        return EnumerableFirstOrDefaultMethodInfo;

                //    case nameof(Enumerable.Single):
                //        return EnumerableSingleMethodInfo;

                //    case nameof(Enumerable.SingleOrDefault):
                //        return EnumerableSingleOrDefaultMethodInfo;

                //    default:
                //        throw new InvalidOperationException("Invalid method name: " + name);
                //}
            }

            throw new InvalidOperationException("Invalid method name: " + name);
        }
    }
}
