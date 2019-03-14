// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public abstract class LinqQueryVisitorBase : ExpressionVisitor
    {
        protected MethodInfo QueryableWhereMethodInfo { get; set; }
        protected MethodInfo QueryableSelectMethodInfo { get; set; }
        protected MethodInfo QueryableOrderByMethodInfo { get; set; }
        protected MethodInfo QueryableOrderByDescendingMethodInfo { get; set; }
        protected MethodInfo QueryableThenByMethodInfo { get; set; }
        protected MethodInfo QueryableThenByDescendingMethodInfo { get; set; }
        protected MethodInfo QueryableJoinMethodInfo { get; set; }
        protected MethodInfo QueryableGroupJoinMethodInfo { get; set; }
        protected MethodInfo QueryableSelectManyMethodInfo { get; set; }
        protected MethodInfo QueryableSelectManyWithResultOperatorMethodInfo { get; set; }

        protected MethodInfo QueryableFirstMethodInfo { get; set; }
        protected MethodInfo QueryableFirstOrDefaultMethodInfo { get; set; }
        protected MethodInfo QueryableSingleMethodInfo { get; set; }
        protected MethodInfo QueryableSingleOrDefaultMethodInfo { get; set; }

        protected MethodInfo QueryableFirstPredicateMethodInfo { get; set; }
        protected MethodInfo QueryableFirstOrDefaultPredicateMethodInfo { get; set; }
        protected MethodInfo QueryableSinglePredicateMethodInfo { get; set; }
        protected MethodInfo QueryableSingleOrDefaultPredicateMethodInfo { get; set; }

        protected MethodInfo QueryableAny { get; set; }
        protected MethodInfo QueryableAnyPredicate { get; set; }
        protected MethodInfo QueryableContains { get; set; }

        protected MethodInfo QueryableCountMethodInfo { get; set; }
        protected MethodInfo QueryableCountPredicateMethodInfo { get; set; }
        protected MethodInfo QueryableDistinctMethodInfo { get; set; }
        protected MethodInfo QueryableTakeMethodInfo { get; set; }
        protected MethodInfo QueryableSkipMethodInfo { get; set; }

        protected MethodInfo QueryableOfType { get; set; }

        protected MethodInfo QueryableDefaultIfEmpty { get; set; }
        protected MethodInfo QueryableDefaultIfEmptyWithDefaultValue { get; set; }

        protected MethodInfo EnumerableWhereMethodInfo { get; set; }
        protected MethodInfo EnumerableSelectMethodInfo { get; set; }

        protected MethodInfo EnumerableGroupJoinMethodInfo { get; set; }
        protected MethodInfo EnumerableSelectManyWithResultOperatorMethodInfo { get; set; }

        protected MethodInfo EnumerableFirstPredicateMethodInfo { get; set; }
        protected MethodInfo EnumerableFirstOrDefaultPredicateMethodInfo { get; set; }
        protected MethodInfo EnumerableSinglePredicateMethodInfo { get; set; }
        protected MethodInfo EnumerableSingleOrDefaultPredicateMethodInfo { get; set; }

        protected MethodInfo EnumerableDefaultIfEmptyMethodInfo { get; set; }

        protected MethodInfo EnumerableAny { get; set; }
        protected MethodInfo EnumerableAnyPredicate { get; set; }

        protected LinqQueryVisitorBase()
        {
            var queryableMethods = typeof(Queryable).GetMethods().ToList();

            QueryableWhereMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Where) && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();
            QueryableSelectMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Select) && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();
            QueryableOrderByMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.OrderBy) && m.GetParameters().Count() == 2).Single();
            QueryableOrderByDescendingMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.OrderByDescending) && m.GetParameters().Count() == 2).Single();
            QueryableThenByMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.ThenBy) && m.GetParameters().Count() == 2).Single();
            QueryableThenByDescendingMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.ThenByDescending) && m.GetParameters().Count() == 2).Single();
            QueryableJoinMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Join) && m.GetParameters().Count() == 5).Single();
            QueryableGroupJoinMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.GroupJoin) && m.GetParameters().Count() == 5).Single();

            QueryableSelectManyMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.SelectMany) && m.GetParameters().Count() == 2 && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();
            QueryableSelectManyWithResultOperatorMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.SelectMany) && m.GetParameters().Count() == 3 && m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Count() == 2).Single();

            QueryableFirstMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.First) && m.GetParameters().Count() == 1).Single();
            QueryableFirstOrDefaultMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.FirstOrDefault) && m.GetParameters().Count() == 1).Single();
            QueryableSingleMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Single) && m.GetParameters().Count() == 1).Single();
            QueryableSingleOrDefaultMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.SingleOrDefault) && m.GetParameters().Count() == 1).Single();

            QueryableFirstPredicateMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.First) && m.GetParameters().Count() == 2).Single();
            QueryableFirstOrDefaultPredicateMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.FirstOrDefault) && m.GetParameters().Count() == 2).Single();
            QueryableSinglePredicateMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Single) && m.GetParameters().Count() == 2).Single();
            QueryableSingleOrDefaultPredicateMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.SingleOrDefault) && m.GetParameters().Count() == 2).Single();

            QueryableCountMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Count) && m.GetParameters().Count() == 1).Single();
            QueryableCountPredicateMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Count) && m.GetParameters().Count() == 2).Single();

            QueryableDistinctMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Distinct) && m.GetParameters().Count() == 1).Single();
            QueryableTakeMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Take) && m.GetParameters().Count() == 2).Single();
            QueryableSkipMethodInfo = queryableMethods.Where(m => m.Name == nameof(Queryable.Skip) && m.GetParameters().Count() == 2).Single();

            QueryableAny = queryableMethods.Where(m => m.Name == nameof(Queryable.Any) && m.GetParameters().Count() == 1).Single();
            QueryableAnyPredicate = queryableMethods.Where(m => m.Name == nameof(Queryable.Any) && m.GetParameters().Count() == 2).Single();
            QueryableContains = queryableMethods.Where(m => m.Name == nameof(Queryable.Contains) && m.GetParameters().Count() == 2).Single();

            QueryableOfType = queryableMethods.Where(m => m.Name == nameof(Queryable.OfType) && m.GetParameters().Count() == 1).Single();

            QueryableDefaultIfEmpty = queryableMethods.Where(m => m.Name == nameof(Queryable.DefaultIfEmpty) && m.GetParameters().Count() == 1).Single();
            QueryableDefaultIfEmptyWithDefaultValue = queryableMethods.Where(m => m.Name == nameof(Queryable.DefaultIfEmpty) && m.GetParameters().Count() == 2).Single();

            var enumerableMethods = typeof(Enumerable).GetMethods().ToList();

            EnumerableWhereMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.Where) && m.GetParameters()[1].ParameterType.GetGenericArguments().Count() == 2).Single();
            EnumerableSelectMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.Select) && m.GetParameters()[1].ParameterType.GetGenericArguments().Count() == 2).Single();

            EnumerableGroupJoinMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.GroupJoin) && m.GetParameters().Count() == 5).Single();
            EnumerableSelectManyWithResultOperatorMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.SelectMany) && m.GetParameters().Count() == 3 && m.GetParameters()[1].ParameterType.GetGenericArguments().Count() == 2).Single();

            EnumerableFirstPredicateMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.First) && m.GetParameters().Count() == 2).Single();
            EnumerableFirstOrDefaultPredicateMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.FirstOrDefault) && m.GetParameters().Count() == 2).Single();
            EnumerableSinglePredicateMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.Single) && m.GetParameters().Count() == 2).Single();
            EnumerableSingleOrDefaultPredicateMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.SingleOrDefault) && m.GetParameters().Count() == 2).Single();

            EnumerableDefaultIfEmptyMethodInfo = enumerableMethods.Where(m => m.Name == nameof(Enumerable.DefaultIfEmpty) && m.GetParameters().Count() == 1).Single();

            EnumerableAny = enumerableMethods.Where(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Count() == 1).Single();
            EnumerableAnyPredicate = enumerableMethods.Where(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Count() == 2).Single();
        }
    }
}
