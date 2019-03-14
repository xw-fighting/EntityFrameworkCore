// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.NavigationExpansion.Visitors
{
    public class NavigationExpansionVisitorBase : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                var newRootParameter = (ParameterExpression)Visit(navigationBindingExpression.RootParameter);

                return newRootParameter != navigationBindingExpression.RootParameter
                    ? new NavigationBindingExpression(
                        newRootParameter,
                        navigationBindingExpression.NavigationTreeNode,
                        navigationBindingExpression.EntityType,
                        navigationBindingExpression.SourceMapping,
                        navigationBindingExpression.Type)
                    : navigationBindingExpression;
            }

            if (extensionExpression is CustomRootExpression customRootExpression)
            {
                var newRootParameter = (ParameterExpression)Visit(customRootExpression.RootParameter);

                return newRootParameter != customRootExpression.RootParameter
                    ? new CustomRootExpression(newRootParameter, customRootExpression.Mapping, customRootExpression.Type)
                    : customRootExpression;
            }

            //if (extensionExpression is CustomRootExpression2 customRootExpression2)
            //{
            //    var newNavigationExpansion = (NavigationExpansionExpression)Visit(customRootExpression2.NavigationExpansion);

            //    return newNavigationExpansion != customRootExpression2.NavigationExpansion
            //        ? new CustomRootExpression2(newNavigationExpansion, customRootExpression2.Mapping, customRootExpression2.Type)
            //        : customRootExpression2;
            //}

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

            //if (extensionExpression is NullSafeEqualExpression nullSafeEqualExpression)
            //{
            //    var newOuterKeyNullCheck = Visit(nullSafeEqualExpression.OuterKeyNullCheck);
            //    var newEqualExpression = (BinaryExpression)Visit(nullSafeEqualExpression.EqualExpression);
            //    var newNavigationRootExpression = Visit(nullSafeEqualExpression.NavigationRootExpression);

            //    return newOuterKeyNullCheck != nullSafeEqualExpression.OuterKeyNullCheck || newEqualExpression != nullSafeEqualExpression.EqualExpression || newNavigationRootExpression != nullSafeEqualExpression.NavigationRootExpression
            //        ? new NullSafeEqualExpression(newOuterKeyNullCheck, newEqualExpression, nullSafeEqualExpression.NavigationRootExpression, nullSafeEqualExpression.Navigations)
            //        : nullSafeEqualExpression;
            //}

            if (extensionExpression is NullConditionalExpression nullConditionalExpression)
            {
                var newCaller = Visit(nullConditionalExpression.Caller);
                var newAccessOperation = Visit(nullConditionalExpression.AccessOperation);

                return newCaller != nullConditionalExpression.Caller || newAccessOperation != nullConditionalExpression.AccessOperation
                    ? new NullConditionalExpression(newCaller, newAccessOperation)
                    : nullConditionalExpression;
            }

            throw new System.InvalidOperationException("Unhandled operator: " + extensionExpression);
        }
    }
}
