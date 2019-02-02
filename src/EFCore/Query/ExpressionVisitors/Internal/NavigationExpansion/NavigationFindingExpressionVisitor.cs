// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query.Expressions.Internal;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal.NavigationExpansion
{
    public class NavigationFindingExpressionVisitor : NavigationExpansionExpressionVisitorBase
    {
        private ParameterExpression _sourceParameter;

        public NavigationFindingExpressionVisitor(
            ParameterExpression sourceParameter)
        {
            _sourceParameter = sourceParameter;
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            if (extensionExpression is NavigationBindingExpression navigationBindingExpression)
            {
                if (navigationBindingExpression.RootParameter == _sourceParameter
                    && navigationBindingExpression.Navigations.Count > 0)
                {
                    var inheritanceRoot = navigationBindingExpression.Navigations[0].ClrType != navigationBindingExpression.RootParameter.Type
                        && navigationBindingExpression.Navigations[0].DeclaringEntityType.GetAllBaseTypes().Any(t => t.ClrType == navigationBindingExpression.RootParameter.Type);

                    var navigationPath = NavigationTreeNode.Create(navigationBindingExpression.Navigations, inheritanceRoot);
                    if (!navigationBindingExpression.SourceMapping.FoundNavigations.Any(p => p.Contains(navigationPath)))
                    {
                        var success = false;
                        foreach (var foundNavigationPath in navigationBindingExpression.SourceMapping.FoundNavigations)
                        {
                            if (!success)
                            {
                                success = foundNavigationPath.TryCombine(navigationPath);
                            }
                        }

                        if (!success)
                        {
                            navigationBindingExpression.SourceMapping.FoundNavigations.Add(navigationPath);
                        }
                    }
                }

                return extensionExpression;
            }

            return base.VisitExtension(extensionExpression);
        }
    }
}
