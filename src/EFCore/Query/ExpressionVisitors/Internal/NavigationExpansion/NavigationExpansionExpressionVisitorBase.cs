// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public abstract class NavigationExpansionExpressionVisitorBase : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                // TODO: hack, navigation binding would convert "naked" ParameterExpression into NavigationBindingExpression
                // in that case, we want to unwrap the root
                var newRootParameter = navigationBindingExpression.RootParameter;
                var newRootParameterVisitResult = Visit(navigationBindingExpression.RootParameter);
                if (newRootParameterVisitResult is NavigationBindingExpression nbe)
                {
                    newRootParameter = nbe.RootParameter;
                }

                var newOperand = Visit(navigationBindingExpression.Operand);

                return newRootParameter != navigationBindingExpression.RootParameter || newOperand != navigationBindingExpression.Operand
                    ? new NavigationBindingExpression(
                        newOperand,
                        newRootParameter,
                        navigationBindingExpression.Navigations.ToList(),
                        navigationBindingExpression.EntityType,
                        navigationBindingExpression.SourceMapping)
                    : navigationBindingExpression;
            }

            if (extensionExpression is NavigationExpansionExpression navigationExpansionExpression)
            {
                var newOperand = Visit(navigationExpansionExpression.Operand);

                return newOperand != navigationExpansionExpression.Operand
                    ? new NavigationExpansionExpression(
                        newOperand,
                        navigationExpansionExpression.State,
                        navigationExpansionExpression.Type)
                    : navigationExpansionExpression;
            }

            if (extensionExpression is NullSafeEqualExpression nullSafeEqualExpression)
            {
                var newOuterKeyNullCheck = Visit(nullSafeEqualExpression.OuterKeyNullCheck);
                var newEqualExpression = (BinaryExpression)Visit(nullSafeEqualExpression.EqualExpression);
                var newNavigationRootExpression = Visit(nullSafeEqualExpression.NavigationRootExpression);

                return newOuterKeyNullCheck != nullSafeEqualExpression.OuterKeyNullCheck || newEqualExpression != nullSafeEqualExpression.EqualExpression || newNavigationRootExpression != nullSafeEqualExpression.NavigationRootExpression
                    ? new NullSafeEqualExpression(newOuterKeyNullCheck, newEqualExpression, nullSafeEqualExpression.NavigationRootExpression, nullSafeEqualExpression.Navigations)
                    : nullSafeEqualExpression;
            }

            return base.VisitExtension(extensionExpression);
        }
    }
}
