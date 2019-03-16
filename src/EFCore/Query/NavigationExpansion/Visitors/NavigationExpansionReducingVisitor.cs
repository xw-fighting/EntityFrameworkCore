// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq.Expressions;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public class NavigationExpansionReducingVisitor : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is CorrelationPredicateExpression correlationPredicateExpression)
            {
                var newOuterKeyNullCheck = Visit(correlationPredicateExpression.OuterKeyNullCheck);
                var newEqualExpression = (BinaryExpression)Visit(correlationPredicateExpression.EqualExpression);
                //var newNavigationRootExpression = Visit(nullSafeEqualExpression.NavigationRootExpression);

                return newOuterKeyNullCheck != correlationPredicateExpression.OuterKeyNullCheck || newEqualExpression != correlationPredicateExpression.EqualExpression// || newNavigationRootExpression != nullSafeEqualExpression.NavigationRootExpression
                    ? new CorrelationPredicateExpression(newOuterKeyNullCheck, newEqualExpression/*, nullSafeEqualExpression.NavigationRootExpression, nullSafeEqualExpression.Navigations*/)
                    : correlationPredicateExpression;
            }

            //if (extensionExpression is NullSafeEqualExpression nullSafeEqualExpression)
            //{
            //    var newOuterKeyNullCheck = Visit(nullSafeEqualExpression.OuterKeyNullCheck);
            //    var newEqualExpression = (BinaryExpression)Visit(nullSafeEqualExpression.EqualExpression);
            //    var newNavigationRootExpression = Visit(nullSafeEqualExpression.NavigationRootExpression);

            //    return newOuterKeyNullCheck != nullSafeEqualExpression.OuterKeyNullCheck || newEqualExpression != nullSafeEqualExpression.EqualExpression || newNavigationRootExpression != nullSafeEqualExpression.NavigationRootExpression
            //        ? new NullSafeEqualExpression(newOuterKeyNullCheck, newEqualExpression, nullSafeEqualExpression.NavigationRootExpression, nullSafeEqualExpression.Navigations)
            //        : nullSafeEqualExpression;
            //}

            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                var result = navigationBindingExpression.NavigationTreeNode.BuildExpression(navigationBindingExpression.RootParameter);

                return result;
            }

            return base.VisitExtension(extensionExpression);
        }
    }
}
