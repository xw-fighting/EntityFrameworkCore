// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using Remotion.Linq.Parsing;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class AnonymousObjectAccessSimplifyingExpressionVisitor : RelinqExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            if (memberExpression.Expression is NewExpression newExpression
                && newExpression.Type.Name.Contains("__AnonymousType"))
            {
                var matchingMemberIndex = newExpression.Members.Select((m, i) => new { match = m == memberExpression.Member, i }).Where(r => r.match).Single().i;

                return newExpression.Arguments[matchingMemberIndex];
            }

            return base.VisitMember(memberExpression);
        }
    }
}
