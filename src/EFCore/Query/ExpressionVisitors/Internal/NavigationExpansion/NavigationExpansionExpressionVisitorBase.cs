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
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression2)
            {
                var newRootParameter = (ParameterExpression)Visit(navigationBindingExpression2.RootParameter);

                return newRootParameter != navigationBindingExpression2.RootParameter
                    ? new NavigationBindingExpression(
                        newRootParameter,
                        navigationBindingExpression2.NavigationTreeNode,
                        navigationBindingExpression2.EntityType,
                        navigationBindingExpression2.SourceMapping,
                        navigationBindingExpression2.Type)
                    : navigationBindingExpression2;
            }

            if (extensionExpression is CustomRootExpression customRootExpression)
            {
                var newRootParameter = (ParameterExpression)Visit(customRootExpression.RootParameter);

                return newRootParameter != customRootExpression.RootParameter
                    ? new CustomRootExpression(newRootParameter, customRootExpression.Mapping, customRootExpression.Type)
                    : customRootExpression;
            }

            if (extensionExpression is CustomRootExpression2 customRootExpression2)
            {
                var newRoot = Visit(customRootExpression2.Root);

                return newRoot != customRootExpression2.Root
                    ? new CustomRootExpression2(newRoot, customRootExpression2.Mapping, customRootExpression2.Type)
                    : customRootExpression2;
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
